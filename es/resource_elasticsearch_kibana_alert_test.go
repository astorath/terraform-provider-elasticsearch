package es

import (
	"context"
	"errors"
	"fmt"
	"testing"

	elastic7 "github.com/olivere/elastic/v7"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func TestAccElasticsearchKibanaAlert(t *testing.T) {
	provider := Provider().(*schema.Provider)
	err := provider.Configure(&terraform.ResourceConfig{})
	if err != nil {
		t.Skipf("err: %s", err)
	}
	meta := provider.Meta()

	esClient, err := getKibanaClient(meta.(*ProviderConf))
	if err != nil {
		t.Skipf("err: %s", err)
	}

	var allowed bool
	switch esClient.(type) {
	case *elastic7.Client:
		allowed = true
	default:
		allowed = false
	}
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			if !allowed {
				t.Skip("Kibana Alerts only supported on ES >= 7.11")
			}
		},
		Providers:    testAccKibanaProviders,
		CheckDestroy: testCheckElasticsearchKibanaAlertDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccElasticsearchKibanaAlert,
				Check: resource.ComposeTestCheckFunc(
					testCheckElasticsearchKibanaAlertExists("elasticsearch_kibana_alert.test"),
				),
			},
		},
	})
}

func TestAccElasticsearchKibanaAlert_importBasic(t *testing.T) {
	provider := Provider().(*schema.Provider)
	err := provider.Configure(&terraform.ResourceConfig{})
	if err != nil {
		t.Skipf("err: %s", err)
	}
	meta := provider.Meta()

	esClient, err := getClient(meta.(*ProviderConf))
	if err != nil {
		t.Skipf("err: %s", err)
	}

	var allowed bool
	switch esClient.(type) {
	case *elastic7.Client:
		allowed = true
	default:
		allowed = false
	}
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			if !allowed {
				t.Skip("Kibana Alerts only supported on ES >= 7.11")
			}
		},
		Providers:    testAccKibanaProviders,
		CheckDestroy: testCheckElasticsearchKibanaAlertDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccElasticsearchKibanaAlert,
			},
			{
				ResourceName:      "elasticsearch_kibana_alert.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func testCheckElasticsearchKibanaAlertExists(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("Not found: %s", name)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No kibana alert ID is set")
		}

		meta := testAccKibanaProvider.Meta()

		esClient, err := getClient(meta.(*ProviderConf))
		if err != nil {
			return err
		}

		switch client := esClient.(type) {
		case *elastic7.Client:
			_, err = client.IndexGetIndexTemplate(rs.Primary.ID).Do(context.TODO())
		default:
			err = errors.New("Kibana Alerts only supported on ES >= 7.11")
		}

		if err != nil {
			return err
		}

		return nil
	}
}

func testCheckElasticsearchKibanaAlertDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "elasticsearch_kibana_alert" {
			continue
		}

		meta := testAccKibanaProvider.Meta()

		esClient, err := getClient(meta.(*ProviderConf))
		if err != nil {
			return err
		}

		switch client := esClient.(type) {
		case *elastic7.Client:
			// todo
			_, err = client.IndexGetTemplate(rs.Primary.ID).Do(context.TODO())
		default:
			err = errors.New("Kibana Alerts only supported on ES >= 7.11")
		}

		if err != nil {
			return nil // should be not found error
		}

		return fmt.Errorf("kibana alert %q still exists", rs.Primary.ID)
	}

	return nil
}

var testAccElasticsearchKibanaAlert = `
resource "elasticsearch_kibana_alert" "test" {
  name = "terraform-test"
}
`
