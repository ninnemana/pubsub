package psmongo

import (
	"fmt"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
)

type ClientConfig struct {
	Client    *pubsub.Client
	Config    *jwt.Config
	ProjectID string
	Context   context.Context
}

var (
	DefaultScopes = []string{
		pubsub.ScopeCloudPlatform,
		pubsub.ScopePubSub,
	}

	psClient = pubsub.NewClient
)

type Admin interface {
	NewClient(email string, key string, project string, scopes []string) (*ClientConfig, error)
}

type Subscriber interface {
	Admin
	ListTopics() ([]pubsub.Topic, error)
}

type Publisher interface {
	Admin
	PushMessage(topic string, msgs ...*pubsub.Message) error
	CreateTopic(topic string) (*pubsub.Topic, error)
}

func NewBackgroundClient(project string) (*ClientConfig, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	config := &ClientConfig{
		Client:    client,
		ProjectID: project,
		Context:   ctx,
	}

	return config, nil
}

// NewClient Generates a http.Client that is authenticated against
// Google Cloud Platform with the `scopes` provided.
func NewClient(email string, key string, project string, scopes []string) (*ClientConfig, error) {
	if len(scopes) == 0 {
		scopes = DefaultScopes
	}
	conf := &jwt.Config{
		Email:      email,
		PrivateKey: []byte(key),
		Scopes:     scopes,
	}

	ctx := context.Background()

	opt := option.WithHTTPClient(conf.Client(oauth2.NoContext))

	client, err := pubsub.NewClient(ctx, project, opt)
	if err != nil {
		return nil, err
	}

	config := &ClientConfig{
		Client:    client,
		Config:    conf,
		ProjectID: project,
		Context:   ctx,
	}

	return config, nil
}

func (client ClientConfig) ListTopics() ([]*pubsub.Topic, error) {
	topics := client.Client.Topics(client.Context)
	if topics == nil {
		return nil, nil
	}

	var tps []*pubsub.Topic
	for {
		var tp *pubsub.Topic
		var err error
		tp, err = topics.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		tps = append(tps, tp)
	}

	return tps, nil
}

// PushMessage Will send the `msgs` to Google PubSub into the `topic` that
// is provided. If the `topic` doesn't exist, it will be created.
func (client ClientConfig) PushMessage(topic string, msgs ...*pubsub.Message) error {

	topics := client.Client.Topics(context.Background())
	if topics == nil {
		return fmt.Errorf("could not find topic %s", topic)
	}

	var t *pubsub.Topic
	for {
		var tpc *pubsub.Topic
		var err error
		tpc, err = topics.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		if tpc.String() == topic {
			t = tpc
			break
		}
	}

	_, err := t.Publish(client.Context, msgs...)

	return err
}

func (client ClientConfig) CreateTopic(topic string) (*pubsub.Topic, error) {
	t := client.Client.Topic(topic)
	if t != nil {
		return t, nil
	}

	return client.Client.CreateTopic(client.Context, topic)
}
