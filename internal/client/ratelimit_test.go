package client

import (
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
)

// recordSleeps replaces the pause between retries and returns the pauses taken.
func recordSleeps(t *testing.T) *[]time.Duration {
	t.Helper()
	var slept []time.Duration
	savedSleep, savedJitter := sleep, jitter
	sleep = func(d time.Duration) { slept = append(slept, d) }
	jitter = func(d time.Duration) time.Duration { return d }
	t.Cleanup(func() { sleep, jitter = savedSleep, savedJitter })
	return &slept
}

// answers makes the endpoint answer 429 the given number of times, then 200.
func answers(limited int, retryAfter string) httpmock.Responder {
	calls := 0
	return func(*http.Request) (*http.Response, error) {
		calls++
		if calls <= limited {
			resp := httpmock.NewStringResponse(http.StatusTooManyRequests, `{"errors": [{"message": "429 Too Many Requests: 50 per 1 second"}]}`)
			if retryAfter != "" {
				resp.Header.Set("Retry-After", retryAfter)
			}
			return resp, nil
		}
		return httpmock.NewJsonResponse(200, map[string]interface{}{"result": []interface{}{}})
	}
}

func TestRateLimitedRequestsAreRetried(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	slept := recordSleeps(t)
	registerLogin("token")
	httpmock.RegisterResponder("GET", testHost+"/api/v1/chart/1", answers(3, ""))

	resp, err := NewClient(testHost, "user", "pass").DoRequest("GET", "/api/v1/chart/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200 after the retries", resp.StatusCode)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	if len(*slept) != len(want) {
		t.Fatalf("paused %v, want %v", *slept, want)
	}
	for i := range want {
		if (*slept)[i] != want[i] {
			t.Fatalf("paused %v, want %v", *slept, want)
		}
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	slept := recordSleeps(t)
	registerLogin("token")
	httpmock.RegisterResponder("GET", testHost+"/api/v1/chart/1", answers(2, "3"))

	resp, err := NewClient(testHost, "user", "pass").DoRequest("GET", "/api/v1/chart/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(*slept) != 2 || (*slept)[0] != 3*time.Second || (*slept)[1] != 3*time.Second {
		t.Fatalf("paused %v, want 3s twice", *slept)
	}
	if got := rateLimitDelay("3600", 0); got != maxRateLimitDelay {
		t.Errorf("a long Retry-After waits %v, want at most %v", got, maxRateLimitDelay)
	}
}

func TestRateLimitedRequestsGiveUp(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	recordSleeps(t)
	registerLogin("token")
	httpmock.RegisterResponder("GET", testHost+"/api/v1/chart/1", answers(100, ""))

	resp, err := NewClient(testHost, "user", "pass").DoRequest("GET", "/api/v1/chart/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d, want the 429 after the last try", resp.StatusCode)
	}
	if n := httpmock.GetCallCountInfo()["GET "+testHost+"/api/v1/chart/1"]; n != maxRateLimitRetries+1 {
		t.Fatalf("sent %d times, want %d", n, maxRateLimitRetries+1)
	}
}

func TestRateLimitedLoginIsRetried(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	recordSleeps(t)
	calls := 0
	httpmock.RegisterResponder("POST", testHost+"/api/v1/security/login", func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return httpmock.NewStringResponse(http.StatusTooManyRequests, "too many"), nil
		}
		return httpmock.NewJsonResponse(200, map[string]string{"access_token": "token"})
	})

	if err := NewClient(testHost, "user", "pass").Authenticate(); err != nil {
		t.Fatalf("the login failed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("logged in %d times, want 2", calls)
	}
}

func TestJitterAddsUpToAQuarter(t *testing.T) {
	for i := 0; i < 100; i++ {
		if d := jitter(4 * time.Second); d < 4*time.Second || d > 5*time.Second {
			t.Fatalf("jitter(4s) = %v, want between 4s and 5s", d)
		}
	}
}
