package pubsub

import (
	"os"

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
	// OAuthEmail Is the e-mail address used to authenticating
	// against Google Cloud Platform
	OAuthEmail = os.Getenv("GOOGLE_OAUTH_EMAIL")
	// ClientKey Is the private key generated from Google Cloud Platform
	// that cooresponds to the OAuthEmail.
	ClientKey = os.Getenv("GOOGLE_CLIENT_KEY")
	ProjectID = os.Getenv("PROJECT_ID")

	pubsubScopes = []string{
		ps.ScopeCloudPlatform,
		ps.ScopePubSub,
	}
)

// NewClient Generates a http.Client that is authenticated against
// Google Cloud Platform with the `scopes` provided.
func NewClient(scopes []string) error {
	if OAuthEmail == "" {
		return backgrounContext(scopes)
	}

	return jwtContext(scopes)
}

func jwtContext(scopes []string) error {
	conf := &jwt.Config{
		Email:      OAuthEmail,
		PrivateKey: []byte(ClientKey),
		Scopes:     scopes,
		TokenURL:   google.JWTTokenURL,
	}

	opt := option.WithHTTPClient(conf.Client(oauth2.NoContext))
	var err error
	client, err = ps.NewClient(context.Background(), ProjectID, opt)

	return err
}

func backgrounContext(scopes []string) error {
	ctx := context.Background()
	c, err := google.DefaultClient(ctx, scopes...)
	if err != nil {
		return err
	}

	client, err = ps.NewClient(ctx, ProjectID, option.WithHTTPClient(c))

	return err
}

// PushMessage Will send the `msgs` to Google PubSub into the `topic` that
// is provided. If the `topic` doesn't exist, it will be created.
func PushMessage(topic string, msgs ...*ps.Message) error {
	var err error
	if pubsubCtx == nil {
		NewClient(pubsubScopes)
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
