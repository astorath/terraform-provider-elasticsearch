package es

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/go-version"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	// "github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/olivere/elastic/uritemplates"

	elastic7 "github.com/olivere/elastic/v7"

	"github.com/phillbaker/terraform-provider-elasticsearch/kibana"
)

var minimalKibanaVersion, _ = version.NewVersion("7.11.0")

func resourceElasticsearchKibanaAlert() *schema.Resource {
	return &schema.Resource{
		Create: resourceElasticsearchKibanaAlertCreate,
		Read:   resourceElasticsearchKibanaAlertRead,
		Update: resourceElasticsearchKibanaAlertUpdate,
		Delete: resourceElasticsearchKibanaAlertDelete,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
			"space_id": {
				Type:     schema.TypeString,
				Optional: true,
				// DiffSuppressFunc: diffSuppressKibanaAlert,
				// ValidateFunc:     validation.StringIsJSON,
			},
			"tags": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"alert_type_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"schedule": {
				Type:     schema.TypeList,
				MaxItems: 1,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"interval": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
			"throttle": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"notify_when": {
				Type:     schema.TypeString,
				Required: true,
			},
			"enabled": {
				Type:        schema.TypeBool,
				Description: "",
				Default:     true,
				Optional:    true,
			},
			"consumer": {
				Type:     schema.TypeString,
				Required: true,
			},
			"params": {
				Type:     schema.TypeMap,
				Optional: true,
				// Default:  {},
			},
			"actions": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"group": {
							Type:     schema.TypeString,
							Required: true,
						},
						"id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"action_type_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"params": {
							Type:     schema.TypeMap,
							Optional: true,
							// Default:  {},
						},
					},
				},
			},
		},
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
	}
}

func resourceElasticsearchKibanaAlertCreate(d *schema.ResourceData, meta interface{}) error {
	err := resourceElasticsearchPostKibanaAlert(d, meta)
	if err != nil {
		return err
	}
	// d.SetId(d.Get("name").(string)) // todo
	return nil
}

func resourceElasticsearchKibanaAlertRead(d *schema.ResourceData, meta interface{}) error {
	id := d.Id()
	space_id := d.Get("space_id").(string)

	var alert kibana.Alert
	var elasticVersion *version.Version

	esClient, err := getKibanaClient(meta.(*ProviderConf))
	if err != nil {
		return err
	}

	switch client := esClient.(type) {
	case *elastic7.Client:
		elasticVersion, err = elastic7GetVersion(client)
		if err == nil {
			if elasticVersion.LessThan(minimalKibanaVersion) {
				err = fmt.Errorf("Kibana Alert endpoint only available from Kibana >= 7.11, got version %s", elasticVersion.String())
			} else {
				alert, err = kibanaGetAlert(client, id, space_id)
			}
		}
	default:
		err = fmt.Errorf("Kibana Alert endpoint only available from Kibana >= 7.11, got version < 7.0.0")
	}
	if err != nil {
		if elastic7.IsNotFound(err) {
			log.Printf("[WARN] Kibana Alert (%s) not found, removing from state", id)
			d.SetId("")
			return nil
		}

		return err
	}

	ds := &resourceDataSetter{d: d}
	ds.set("name", alert.Name)
	ds.set("tags", alert.Tags)
	ds.set("alert_type_id", alert.AlertTypeID)
	ds.set("schedule", alert.Schedule)
	ds.set("throttle", alert.Throttle)
	ds.set("notify_when", alert.NotifyWhen)
	ds.set("enabled", alert.Enabled)
	ds.set("consumer", alert.Consumer)
	ds.set("params", alert.Params)
	ds.set("actions", alert.Actions)

	return ds.err
}

func resourceElasticsearchKibanaAlertUpdate(d *schema.ResourceData, meta interface{}) error {
	return resourceElasticsearchPutKibanaAlert(d, meta)
}

func resourceElasticsearchKibanaAlertDelete(d *schema.ResourceData, meta interface{}) error {
	id := d.Id()
	space_id := d.Get("space_id").(string)

	var elasticVersion *version.Version

	esClient, err := getKibanaClient(meta.(*ProviderConf))
	if err != nil {
		return err
	}

	switch client := esClient.(type) {
	case *elastic7.Client:
		elasticVersion, err = elastic7GetVersion(client)
		if err == nil {
			if elasticVersion.LessThan(minimalKibanaVersion) {
				err = fmt.Errorf("Kibana Alert endpoint only available from ElasticSearch >= 7.11, got version %s", elasticVersion.String())
			} else {
				err = kibanaDeleteAlert(client, id, space_id)
			}
		}
	default:
		err = fmt.Errorf("Kibana Alert endpoint only available from ElasticSearch >= 7.11, got version < 7.0.0")
	}

	if err != nil {
		return err
	}
	d.SetId("")
	return nil
}

func resourceElasticsearchPostKibanaAlert(d *schema.ResourceData, meta interface{}) error {
	id := d.Id()
	space_id := d.Get("space_id").(string)

	var elasticVersion *version.Version

	esClient, err := getKibanaClient(meta.(*ProviderConf))
	if err != nil {
		return err
	}

	alertSchedule := d.Get("schedule").(*schema.Set).List()[0].(map[string]interface{})
	schedule := kibana.AlertSchedule{
		Interval: alertSchedule["interval"].(string),
	}
	actions, err := expandKibanaActionsList(d.Get("actions").(*schema.Set).List())
	if err != nil {
		return err
	}

	alert := kibana.Alert{
		Name:        d.Get("name").(string),
		Tags:        d.Get("tags").([]string),
		AlertTypeID: d.Get("alert_type_id").(string),
		Schedule:    schedule,
		Throttle:    d.Get("throttle").(string),
		NotifyWhen:  d.Get("notify_when").(string),
		Enabled:     d.Get("enabled").(bool),
		Consumer:    d.Get("consumer").(string),
		Params:      d.Get("params").(map[string]interface{}),
		Actions:     actions,
	}

	switch client := esClient.(type) {
	case *elastic7.Client:
		elasticVersion, err = elastic7GetVersion(client)
		if err == nil {
			if elasticVersion.LessThan(minimalKibanaVersion) {
				err = fmt.Errorf("Kibana Alert endpoint only available from ElasticSearch >= 7.11, got version %s", elasticVersion.String())
			} else {
				err = kibanaPostAlert(client, id, space_id, alert)
			}
		}
	default:
		err = fmt.Errorf("Kibana Alert endpoint only available from ElasticSearch >= 7.11, got version < 7.0.0")
	}

	return err
}

func resourceElasticsearchPutKibanaAlert(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func kibanaGetAlert(client *elastic7.Client, id, space_id string) (kibana.Alert, error) {
	// TODO use space id
	path, err := uritemplates.Expand("/api/alerts/alert/{id}", map[string]string{
		"id": id,
	})
	if err != nil {
		return kibana.Alert{}, fmt.Errorf("error building URL path for alert: %+v", err)
	}

	var body json.RawMessage
	var res *elastic7.Response
	res, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
		Method: "GET",
		Path:   path,
	})
	body = res.Body

	if err != nil {
		return kibana.Alert{}, err
	}

	alert := new(kibana.Alert)
	if err := json.Unmarshal(body, alert); err != nil {
		return *alert, fmt.Errorf("error unmarshalling monitor body: %+v: %+v", err, body)
	}

	return *alert, nil
}

func kibanaPostAlert(client *elastic7.Client, id, space_id string, alert kibana.Alert) error {
	path, err := uritemplates.Expand("api/alerts/alert", map[string]string{
		"id": id,
	})
	if err != nil {
		return fmt.Errorf("error building URL path for alert: %+v", err)
	}

	body, err := json.Marshal(alert)
	if err != nil {
		log.Printf("[INFO] Body: %+v", alert)
		return fmt.Errorf("Body Error: %s", err)
	}

	// var res *elastic7.Response
	_, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
		Method: "POST",
		Path:   path,
		Body:   string(body[:]),
	})

	if err != nil {
		return err
	}

	return nil
}

func kibanaDeleteAlert(client *elastic7.Client, id, space_id string) error {
	// TODO use space id
	path, err := uritemplates.Expand("/api/alerts/alert/{id}", map[string]string{
		"id": id,
	})
	if err != nil {
		return fmt.Errorf("error building URL path for alert: %+v", err)
	}

	_, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
		Method: "DELETE",
		Path:   path,
	})

	if err != nil {
		return err
	}

	return nil
}
