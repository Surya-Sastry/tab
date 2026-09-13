package auth

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPasswordHashRoundTrip(t *testing.T) {
	value := strings.Repeat("test-character-", 2)
	hash, err := HashPassword(value)
	if err != nil {
		t.Fatal(err)
	}
	if hash == value || !VerifyPassword(hash, value) {
		t.Fatal("password hash did not verify")
	}
	if VerifyPassword(hash, value+"different") {
		t.Fatal("different password verified")
	}
}

func TestJWTClaims(t *testing.T) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	key := base64.RawURLEncoding.EncodeToString(random)
	userID := uuid.NewString()
	token, err := Issue(key, userID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Parse(key, token)
	if err != nil || got != userID {
		t.Fatalf("got user=%q err=%v", got, err)
	}
}
