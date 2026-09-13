package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/argon2"
)

const (
	memory      = 64 * 1024
	iterations  = 3
	parallelism = 2
	saltLength  = 16
	keyLength   = 32
)

func HashPassword(password string) (string, error) {
	if len(password) < 12 || len(password) > 256 {
		return "", fmt.Errorf("password must be 12-256 characters")
	}
	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, keyLength)
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		memory, iterations, parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash)), nil
}

func VerifyPassword(encoded, password string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		return false
	}
	var m uint32
	var t uint32
	var p uint8
	for _, item := range strings.Split(parts[3], ",") {
		pair := strings.SplitN(item, "=", 2)
		if len(pair) != 2 {
			return false
		}
		value, err := strconv.ParseUint(pair[1], 10, 32)
		if err != nil {
			return false
		}
		switch pair[0] {
		case "m":
			m = uint32(value)
		case "t":
			t = uint32(value)
		case "p":
			p = uint8(value)
		}
	}
	if m == 0 || t == 0 || p == 0 || m > 256*1024 || t > 10 || p > 16 {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(want) == 0 {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, t, m, p, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

type Claims struct {
	UserID string `json:"uid"`
	jwt.RegisteredClaims
}

func Issue(signingKey, userID string, ttl time.Duration) (string, error) {
	now := time.Now().UTC()
	claims := Claims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: userID, IssuedAt: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			Issuer:    "tab", Audience: jwt.ClaimStrings{"tab-api"},
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signingKey))
}

func Parse(signingKey, value string) (string, error) {
	token, err := jwt.ParseWithClaims(value, &Claims{}, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return []byte(signingKey), nil
	}, jwt.WithAudience("tab-api"), jwt.WithIssuer("tab"))
	if err != nil || !token.Valid {
		return "", errors.New("invalid token")
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || claims.UserID == "" || claims.Subject != claims.UserID {
		return "", errors.New("invalid claims")
	}
	return claims.UserID, nil
}
