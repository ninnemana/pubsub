package pubsub

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewContext(t *testing.T) {
	Convey("Testing NewContext(scopes ...string)", t, func() {
		Convey("should create context", func() {
			NewContext([]string{})
			So(pubsubCtx, ShouldNotBeNil)
			So(pubsubCtx.Err(), ShouldBeNil)
			_, ok := pubsubCtx.Deadline()
			So(ok, ShouldBeFalse)
		})
	})
}

func TestPushMessage(t *testing.T) {
	Convey("Testing PushMessge(topic string, msgs ...*pubsub.Mesage)", t, func() {
		Convey("should error with no scopes in context", func() {
			tmpScopes := pubsubScopes
			pubsubCtx = nil
			err := PushMessage("test_topic", nil)
			So(err, ShouldNotBeNil)

			pubsubScopes = tmpScopes
		})
	})
}
