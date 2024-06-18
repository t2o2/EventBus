package EventBus

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	bus := New()
	if bus == nil {
		t.Log("New EventBus not created!")
		t.Fail()
	}
}

func TestHasCallback(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func() {})
	if bus.HasCallback("topic_topic") {
		t.Fail()
	}
	if !bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSubscribe(t *testing.T) {
	bus := New()
	if bus.Subscribe("topic", func() {}) != nil {
		t.Fail()
	}
	if bus.Subscribe("topic", "String") == nil {
		t.Fail()
	}
}

func TestSubscribeOnce(t *testing.T) {
	bus := New()
	if bus.SubscribeOnce("topic", func() {}) != nil {
		t.Fail()
	}
	if bus.SubscribeOnce("topic", "String") == nil {
		t.Fail()
	}
}

func TestSubscribeOnceAndManySubscribe(t *testing.T) {
	bus := New()
	event := "topic"
	flag := 0
	fn := func() { flag += 1 }
	bus.SubscribeOnce(event, fn)
	bus.Subscribe(event, fn)
	bus.Subscribe(event, fn)
	bus.Publish(event)

	if flag != 3 {
		t.Fail()
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := New()
	handler := func() {}
	bus.Subscribe("topic", handler)
	if bus.Unsubscribe("topic", handler) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic", handler) == nil {
		t.Fail()
	}
}

type handler struct {
	val int
}

func (h *handler) Handle() {
	h.val++
}

func TestUnsubscribeMethod(t *testing.T) {
	bus := New()
	h := &handler{val: 0}

	bus.Subscribe("topic", h.Handle)
	bus.Publish("topic")
	if bus.Unsubscribe("topic", h.Handle) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic", h.Handle) == nil {
		t.Fail()
	}
	bus.Publish("topic")
	bus.WaitAsync()

	if h.val != 1 {
		t.Fail()
	}
}

func TestPublish(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(a int, err error) {
		if a != 10 {
			t.Fail()
		}

		if err != nil {
			t.Fail()
		}
	})
	bus.Publish("topic", 10, nil)
}

func TestSubcribeOnceAsync(t *testing.T) {
	results := make([]int, 0)

	bus := New()
	bus.SubscribeOnceAsync("topic", func(a int, out *[]int) {
		*out = append(*out, a)
	})

	bus.Publish("topic", 10, &results)
	bus.Publish("topic", 10, &results)

	bus.WaitAsync()

	if len(results) != 1 {
		t.Fail()
	}

	if bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSubscribeAsyncTransactional(t *testing.T) {
	results := make([]int, 0)

	bus := New()
	bus.SubscribeAsync("topic", func(a int, out *[]int, dur string) {
		sleep, _ := time.ParseDuration(dur)
		time.Sleep(sleep)
		*out = append(*out, a)
	}, true)

	bus.Publish("topic", 1, &results, "1s")
	bus.Publish("topic", 2, &results, "0s")

	bus.WaitAsync()

	if len(results) != 2 {
		t.Fail()
	}

	if results[0] != 1 || results[1] != 2 {
		t.Fail()
	}
}

func TestSubscribeAsync(t *testing.T) {
	results := make(chan int)

	bus := New()
	_ = bus.SubscribeAsync("topic", func(a int, out chan<- int) {
		out <- a
	}, false)

	bus.Publish("topic", 1, results)
	bus.Publish("topic", 2, results)

	numResults := 0

	go func() {
		for _ = range results {
			numResults++
		}
	}()

	bus.WaitAsync()
	println(2)

	time.Sleep(10 * time.Millisecond)

	// todo race detected during execution of test
	//if numResults != 2 {
	//	t.Fail()
	//}
}

func TestRequestReply(t *testing.T) {
	bus := New()
	_ = bus.SubscribeReplyAsync("topic", func(replyTopic string, action string, in1 float64, in2 float64) {
		var result float64
		switch action {
		case "add":
			result = in1 + in2
		case "sub":
			result = in1 - in2
		case "mul":
			result = in1 * in2
		case "div":
			result = in1 / in2
		}
		fmt.Printf("received: %#v %#v %#v = %#v\n", action, in1, in2, result)
		bus.Publish(replyTopic, result)
	})

	counter := 0

	replyHandler := func(data float64) {
		fmt.Printf("response: %#v\n", data)
		switch counter {
		case 0:
			assert.Equal(t, 22.0, data)
		case 1:
			assert.Equal(t, 2.0, data)
		case 2:
			assert.Equal(t, 120.0, data)
		case 3:
			assert.Equal(t, 1.2, data)
		default:
			assert.Fail(t, "unexpected response")
		}
		counter++
	}

	_ = bus.Request("topic", replyHandler, 10*time.Millisecond, "add", 12.0, 10.0)
	_ = bus.Request("topic", replyHandler, 10*time.Millisecond, "sub", 12.0, 10.0)
	_ = bus.Request("topic", replyHandler, 10*time.Millisecond, "mul", 12.0, 10.0)
	_ = bus.Request("topic", replyHandler, 10*time.Millisecond, "div", 12.0, 10.0)

	time.Sleep(10 * time.Millisecond)
}

func TestConcurrencyReply(t *testing.T) {
	bus := New()
	err := bus.SubscribeReplyAsync("concurrency", func(replyTopic string, in1 float64, in2 float64) {
		time.Sleep(100 * time.Microsecond)
		bus.Publish(replyTopic, in1, in2, in1+in2)
	})
	if err != nil {
		assert.Fail(t, "failed to subscribe")
	}
	counter := atomic.Uint64{}
	replyHandler := func(in1, in2, data float64) {
		assert.Equal(t, in1+in2, data, "wrong value")
		counter.Add(1)
	}
	errCounter := atomic.Uint64{}
	for i := 0; i < 10000; i++ {
		go func() {
			err = bus.Request("concurrency", replyHandler, 200*time.Microsecond, float64(i), float64(10*i))
			if err != nil {
				errCounter.Add(1)
			}
		}()
	}

	time.Sleep(2 * time.Second)
	fmt.Printf("counter: %d error: %d\n", counter.Load(), errCounter.Load())
	assert.Equal(t, 10000, int(counter.Load()+errCounter.Load()), "wrong counter")
}

func TestConcurrentPubSub(t *testing.T) {
	bus := New()
	counter := atomic.Int64{}
	bus.SubscribeAsync("concurrent", func() {
		counter.Add(1)
	}, false)
	for i := 0; i < 10000; i++ {
		go bus.Publish("concurrent")
	}
	bus.WaitAsync()
	assert.Equal(t, 10000, int(counter.Load()), "wrong counter")
}

func TestFailedRequestReply(t *testing.T) {
	bus := New()
	err := bus.SubscribeReplyAsync("topic", func(replyTopic int, action string, in1 float64, in2 float64) {})
	if err != nil {
		fmt.Println(err)
	} else {
		t.Fail()
	}
}

func TestRequestReplyTimeout(t *testing.T) {
	bus := New()
	slowCalculator := func(reply string, a, b int) {
		time.Sleep(1 * time.Second)
		bus.Publish(reply, a+b)
	}

	_ = bus.SubscribeReplyAsync("main:slow_calculator", slowCalculator)

	err := bus.Request("main:slow_calculator", func(rslt int) {
		fmt.Printf("Result: %d\n", rslt)
	}, 10*time.Millisecond, 20, 60)
	assert.NotNil(t, err, "Request should return timeout error")

	err = bus.Request("main:slow_calculator", func(rslt int) {
		fmt.Printf("Result: %d\n", rslt)
	}, 2*time.Second, 20, 90)
	assert.Nil(t, err, "Request should not return an error")

	time.Sleep(100 * time.Millisecond)
}
