package pubsub

import (
	"google.golang.org/api/option"

	ps "cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
)

var (
	client    *ps.Client
	pubsubCtx context.Context

	pubsubScopes = []string{
		ps.ScopeCloudPlatform,
		ps.ScopePubSub,
	}
)

// NewClient Generates a http.Client that is authenticated against
// Google Cloud Platform with the `scopes` provided.
func NewClient(scopes []string, email string, key string, project string) error {
	if email == "" {
		return backgrounContext(scopes, project)
	}

	return jwtContext(scopes, email, key, project)
}

func jwtContext(scopes []string, email string, key string, project string) error {
	conf := &jwt.Config{
		Email:      email,
		PrivateKey: []byte(key),
		Scopes:     scopes,
		TokenURL:   google.JWTTokenURL,
	}

	opt := option.WithHTTPClient(conf.Client(oauth2.NoContext))
	var err error
	client, err = ps.NewClient(context.Background(), project, opt)

	return err
}

func backgrounContext(scopes []string, project string) error {
	ctx := context.Background()
	c, err := google.DefaultClient(ctx, scopes...)
	if err != nil {
		return err
	}

	client, err = ps.NewClient(ctx, project, option.WithHTTPClient(c))

	return err
}

// PushMessage Will send the `msgs` to Google PubSub into the `topic` that
// is provided. If the `topic` doesn't exist, it will be created.
func PushMessage(email, key, project, topic string, msgs ...*ps.Message) error {
	var err error
	if pubsubCtx == nil {
		err = NewClient(pubsubScopes, email, key, project)
		if err != nil {
			return err
		}
	}

	var t *ps.Topic
	t, err = createTopic(topic)
	if err != nil {
		return err
	}

	_, err = t.Publish(pubsubCtx, msgs...)
	return err

}

func createTopic(topic string) (*ps.Topic, error) {
	t := client.Topic(topic)
	if t != nil {
		return t, nil
	}

	return client.CreateTopic(pubsubCtx, topic)
}
