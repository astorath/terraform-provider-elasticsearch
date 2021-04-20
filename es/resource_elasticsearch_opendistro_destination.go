package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/structure"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/olivere/elastic/uritemplates"

	elastic7 "github.com/olivere/elastic/v7"
	elastic6 "gopkg.in/olivere/elastic.v6"
)

const DESTINATION_TYPE = "_doc"
const DESTINATION_INDEX = ".opendistro-alerting-config"

var openDistroDestinationSchema = map[string]*schema.Schema{
	"body": {
		Type:             schema.TypeString,
		Required:         true,
		DiffSuppressFunc: diffSuppressDestination,
		ValidateFunc:     validation.StringIsJSON,
		StateFunc: func(v interface{}) string {
			json, _ := structure.NormalizeJsonString(v)
			return json
		},
		Description: "The JSON body of the destination.",
	},
}

func resourceElasticsearchDeprecatedDestination() *schema.Resource {
	return &schema.Resource{
		Create: resourceElasticsearchOpenDistroDestinationCreate,
		Read:   resourceElasticsearchOpenDistroDestinationRead,
		Update: resourceElasticsearchOpenDistroDestinationUpdate,
		Delete: resourceElasticsearchOpenDistroDestinationDelete,
		Schema: openDistroDestinationSchema,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		DeprecationMessage: "elasticsearch_destination is deprecated, please use elasticsearch_opendistro_destination resource instead.",
	}
}

func resourceElasticsearchOpenDistroDestination() *schema.Resource {
	return &schema.Resource{
		Description: "Provides an Elasticsearch OpenDistro destination, a reusable communication channel for an action, such as email, Slack, or a webhook URL. Please refer to the OpenDistro [destination documentation](https://opendistro.github.io/for-elasticsearch-docs/docs/alerting/monitors/#create-destinations) for details.",
		Create:      resourceElasticsearchOpenDistroDestinationCreate,
		Read:        resourceElasticsearchOpenDistroDestinationRead,
		Update:      resourceElasticsearchOpenDistroDestinationUpdate,
		Delete:      resourceElasticsearchOpenDistroDestinationDelete,
		Schema:      openDistroDestinationSchema,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
	}
}

func resourceElasticsearchOpenDistroDestinationCreate(d *schema.ResourceData, m interface{}) error {
	res, err := resourceElasticsearchOpenDistroPostDestination(d, m)

	if err != nil {
		log.Printf("[INFO] Failed to put destination: %+v", err)
		return err
	}

	d.SetId(res.ID)
	destination, err := json.Marshal(res.Destination)
	if err != nil {
		return err
	}
	err = d.Set("body", string(destination))
	return err
}

func resourceElasticsearchOpenDistroDestinationRead(d *schema.ResourceData, m interface{}) error {
	destination, err := resourceElasticsearchOpenDistroGetDestination(d.Id(), m)

	if elastic6.IsNotFound(err) || elastic7.IsNotFound(err) {
		log.Printf("[WARN] Destination (%s) not found, removing from state", d.Id())
		d.SetId("")
		return nil
	}

	if err != nil {
		return err
	}

	body, err := json.Marshal(destination)
	if err != nil {
		return err
	}

	err = d.Set("body", string(body))
	return err
}

func resourceElasticsearchOpenDistroDestinationUpdate(d *schema.ResourceData, m interface{}) error {
	_, err := resourceElasticsearchOpenDistroPutDestination(d, m)

	if err != nil {
		return err
	}

	return resourceElasticsearchOpenDistroDestinationRead(d, m)
}

func resourceElasticsearchOpenDistroDestinationDelete(d *schema.ResourceData, m interface{}) error {
	var err error

	path, err := uritemplates.Expand("/_opendistro/_alerting/destinations/{id}", map[string]string{
		"id": d.Id(),
	})
	if err != nil {
		return fmt.Errorf("error building URL path for destination: %+v", err)
	}

	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return err
	}
	switch client := esClient.(type) {
	case *elastic7.Client:
		_, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "DELETE",
			Path:   path,
		})
	case *elastic6.Client:
		_, err = client.PerformRequest(context.TODO(), elastic6.PerformRequestOptions{
			Method: "DELETE",
			Path:   path,
		})
	default:
		err = errors.New("destination resource not implemented prior to Elastic v6")
	}

	return err
}

func resourceElasticsearchOpenDistroGetDestination(destinationID string, m interface{}) (Destination, error) {
	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return Destination{}, err
	}

	var dr destinationResponse
	switch client := esClient.(type) {
	case *elastic7.Client:
		// See https://github.com/opendistro-for-elasticsearch/alerting/issues/56,
		// no API endpoint for retrieving destination prior to ODFE 1.11.0. So do
		// a request, if it 404s, fall back to trying to query the index.
		path, err := uritemplates.Expand("/_opendistro/_alerting/destinations/{id}", map[string]string{
			"id": destinationID,
		})
		if err != nil {
			return Destination{}, fmt.Errorf("error building URL path for destination: %+v", err)
		}

		httpResponse, err := client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "GET",
			Path:   path,
		})
		if err == nil {
			var drg destinationResponseGet
			if err := json.Unmarshal(httpResponse.Body, &drg); err != nil {
				return Destination{}, fmt.Errorf("error unmarshalling destination body: %+v", err)
			}
			// The response structure from the API is the same for the index and get
			// endpoints :|, and different from the other endpoints. Normalize the
			// response here.
			log.Printf("[INFO] destination response: %+v", drg)
			// // 2021/04/24 21:52:50 [INFO] destination response:
			// // map[destinations:[map[id:_XXeBXkBWX0T7YR5-vLZ
			// // last_update_time:1.619301169875e+12 name:my-destination
			// // primary_term:1 schema_version:3 seq_no:1
			// // slack:map[url:http://www.example.com] type:slack
			// // user:map[backend_roles:[admin] custom_attribute_names:[] name:admin
			// // roles:[all_access own_index]]]] totalDestinations:1]
			// destinations := m["destinations"].([]interface{})
			// var d Destination
			// if err := json.Unmarshal(destinations[0], &d); err != nil {
			// 	return "", fmt.Errorf("error unmarshalling destination body: %+v", err)
			// }
			// m["destination"] = d
			// m["_id"] = m
			// body, err = json.Marshal(m)
			// if err != nil {
			// 	return "", err
			// }
			if len(drg.Destinations) > 0 {
				return drg.Destinations[0], nil
			} else {
				return Destination{}, fmt.Errorf("endpoint returned empty set of destinations: %+v", drg)
			}
		} else {
			body, err := elastic7GetObject(client, DESTINATION_INDEX, destinationID)

			if err != nil {
				return Destination{}, err
			}
			if err := json.Unmarshal(*body, &dr); err != nil {
				return Destination{}, fmt.Errorf("error unmarshalling destination body: %+v: %+v", err, body)
			}
			return dr.Destination, nil
		}
	case *elastic6.Client:
		body, err := elastic6GetObject(client, DESTINATION_TYPE, DESTINATION_INDEX, destinationID)
		if err != nil {
			return Destination{}, err
		}
		if err := json.Unmarshal(*body, &dr); err != nil {
			return Destination{}, fmt.Errorf("error unmarshalling destination body: %+v: %+v", err, body)
		}
		return dr.Destination, nil
	default:
		return Destination{}, errors.New("destination resource not implemented prior to Elastic v6")
	}
}

func resourceElasticsearchOpenDistroPostDestination(d *schema.ResourceData, m interface{}) (*destinationResponse, error) {
	destinationJSON := d.Get("body").(string)

	var err error
	response := new(destinationResponse)

	path := "/_opendistro/_alerting/destinations/"

	var body json.RawMessage
	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return nil, err
	}
	switch client := esClient.(type) {
	case *elastic7.Client:
		var res *elastic7.Response
		res, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "POST",
			Path:   path,
			Body:   destinationJSON,
		})
		body = res.Body
	case *elastic6.Client:
		var res *elastic6.Response
		res, err = client.PerformRequest(context.TODO(), elastic6.PerformRequestOptions{
			Method: "POST",
			Path:   path,
			Body:   destinationJSON,
		})
		body = res.Body
	default:
		err = errors.New("destination resource not implemented prior to Elastic v6")
	}

	if err != nil {
		return response, err
	}

	if err := json.Unmarshal(body, response); err != nil {
		return response, fmt.Errorf("error unmarshalling destination body: %+v: %+v", err, body)
	}

	return response, nil
}

func resourceElasticsearchOpenDistroPutDestination(d *schema.ResourceData, m interface{}) (*destinationResponse, error) {
	destinationJSON := d.Get("body").(string)

	var err error
	response := new(destinationResponse)

	path, err := uritemplates.Expand("/_opendistro/_alerting/destinations/{id}", map[string]string{
		"id": d.Id(),
	})
	if err != nil {
		return response, fmt.Errorf("error building URL path for destination: %+v", err)
	}

	var body json.RawMessage
	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return nil, err
	}
	switch client := esClient.(type) {
	case *elastic7.Client:
		var res *elastic7.Response
		res, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "PUT",
			Path:   path,
			Body:   destinationJSON,
		})
		body = res.Body
	case *elastic6.Client:
		var res *elastic6.Response
		res, err = client.PerformRequest(context.TODO(), elastic6.PerformRequestOptions{
			Method: "PUT",
			Path:   path,
			Body:   destinationJSON,
		})
		body = res.Body
	default:
		err = errors.New("destination resource not implemented prior to Elastic v6")
	}

	if err != nil {
		return response, err
	}

	if err := json.Unmarshal(body, response); err != nil {
		return response, fmt.Errorf("error unmarshalling destination body: %+v: %+v", err, body)
	}

	return response, nil
}

type destinationResponse struct {
	Version     int         `json:"_version"`
	ID          string      `json:"_id"`
	Destination Destination `json:"destination"`
}

// When this api endpoint was introduced after the other endpoints, it has a
// different response structure
type destinationResponseGet struct {
	Destinations []Destination `json:"destinations"`
}

type Destination struct {
	ID            string      `json:"id"`
	Type          string      `json:"type"`
	Name          string      `json:"name"`
	Slack         interface{} `json:"slack,omitempty"`
	CustomWebhook interface{} `json:"custom_webhook,omitempty"`
	Chime         interface{} `json:"chime,omitempty"`
	SNS           interface{} `json:"sns,omitempty"`
	Email         interface{} `json:"email,omitempty"`
}
