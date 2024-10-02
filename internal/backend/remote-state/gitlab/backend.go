package gitlab

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/opentofu/opentofu/internal/backend"
	"github.com/opentofu/opentofu/internal/encryption"
	"github.com/opentofu/opentofu/internal/legacy/helper/schema"
	"github.com/opentofu/opentofu/internal/logging"
	"github.com/shurcooL/graphql"
	"golang.org/x/oauth2"
)

var (
	defaultApiUrlEnvVars  = []string{"TF_GITLAB_API_URL", "CI_API_V4_URL"}
	defaultProjectEnvVars = []string{"TF_GITLAB_PROJECT", "CI_PROJECT_ID"}
	defaultTokenEnvVars   = []string{"TF_GITLAB_TOKEN", "CI_JOB_TOKEN"}
)

func New(enc encryption.StateEncryption) backend.Backend {
	s := &schema.Backend{
		Schema: map[string]*schema.Schema{
			"address": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.MultiEnvDefaultFunc(defaultApiUrlEnvVars, nil),
				Description: "The Gitlab URL.",
			},
			"project": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.MultiEnvDefaultFunc(defaultProjectEnvVars, nil),
				Description: "The Gitlab project path or ID number.",
			},
			"token": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.MultiEnvDefaultFunc(defaultTokenEnvVars, nil),
				Description: "The Gitlab access token to manage remote state.",
			},
			"retry_max": &schema.Schema{
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_RETRY_MAX", 2),
				Description: "The number of HTTP request retries.",
			},
			"retry_wait_min": &schema.Schema{
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_RETRY_WAIT_MIN", 1),
				Description: "The minimum time in seconds to wait between HTTP request attempts.",
			},
			"retry_wait_max": &schema.Schema{
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_RETRY_WAIT_MAX", 30),
				Description: "The maximum time in seconds to wait between HTTP request attempts.",
			},
		},
	}

	b := &Backend{Backend: s, encryption: enc}
	b.Backend.ConfigureFunc = b.configure
	return b
}

type Backend struct {
	encryption encryption.StateEncryption
	client     *RemoteClient

	address *url.URL
	project string

	httpClient    *http.Client
	graphqlClient *graphql.Client

	*schema.Backend
}

func (b *Backend) configure(ctx context.Context) (err error) {
	data := schema.FromContextBackendConfig(ctx)

	address := data.Get("address").(string)

	if b.address, err = url.Parse(address); err != nil {
		return fmt.Errorf("could not to parse gitlab address %s: %w", address, err)
	} else if b.address.Scheme != "http" && b.address.Scheme != "https" {
		return fmt.Errorf("scheme must be HTTP or HTTPS")
	}

	project := data.Get("project").(string)

	if b.project = project; project == "" {
		return fmt.Errorf("project must not be empty: %s", project)
	}

	httpClient := retryablehttp.NewClient()

	// set up retries like the http backend
	httpClient.RetryMax = data.Get("retry_max").(int)
	httpClient.RetryWaitMin = time.Duration(data.Get("retry_wait_min").(int)) * time.Second
	httpClient.RetryWaitMax = time.Duration(data.Get("retry_wait_max").(int)) * time.Second
	httpClient.Logger = log.New(logging.LogOutput(), "", log.Flags())

	// build an http client with retries and authentication
	b.httpClient = &http.Client{
		Transport: &oauth2.Transport{
			Base: &retryablehttp.RoundTripper{Client: httpClient},
			Source: oauth2.StaticTokenSource(&oauth2.Token{
				TokenType:   "Bearer",
				AccessToken: data.Get("token").(string),
			}),
		},
	}

	// build a graphql client using our http client
	graphqlEndpoint := b.address.JoinPath("api/graphql").String()
	b.graphqlClient = graphql.NewClient(graphqlEndpoint, b.httpClient)

	// build a remote client for the default state
	b.client = b.remoteClientFor(backend.DefaultStateName)

	return nil
}

func (b *Backend) remoteClientFor(stateName string) *RemoteClient {
	return &RemoteClient{
		BaseURL:       b.address,
		Project:       b.project,
		StateName:     stateName,
		HTTPClient:    b.httpClient,
		GraphQLClient: b.graphqlClient,
	}
}
