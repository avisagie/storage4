package names

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateBucket(t *testing.T) {
	ok := []string{"a", "A", "0", "docs-2026-10-29", "a.b_c-d", "_x", "a..b", strings.Repeat("a", 128)}
	bad := []string{"", ".", "..", "..x", ".a", "-a", "a/b", "a b", "a:b", "ä", "a\x00", "a\n", strings.Repeat("a", 129), "a%2Fb"}
	for _, s := range ok {
		if err := ValidateBucket(s); err != nil {
			t.Errorf("ValidateBucket(%q) = %v, want nil", s, err)
		}
	}
	for _, s := range bad {
		if err := ValidateBucket(s); !errors.Is(err, ErrInvalidBucket) {
			t.Errorf("ValidateBucket(%q) = %v, want ErrInvalidBucket", s, err)
		}
	}
}

func TestValidateName(t *testing.T) {
	ok := []string{
		"a", "a/b", "2026-10-29T12:00:00Z/6f1c/page", "...", "a/...", ".hidden", "-x", "A/b", "a.b/c_d",
		strings.Repeat("a", 512), strings.Repeat("a/", 255) + "a",
	}
	bad := []string{
		"", "/", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "./a", "a/..", "a b", "a\\b", "a%2Fb",
		"a?b", "a#b", "ä", "a\x00", strings.Repeat("a", 513), strings.Repeat("é", 200),
	}
	for _, s := range ok {
		if err := ValidateName(s); err != nil {
			t.Errorf("ValidateName(%.40q) = %v, want nil", s, err)
		}
	}
	for _, s := range bad {
		if err := ValidateName(s); !errors.Is(err, ErrInvalidName) {
			t.Errorf("ValidateName(%.40q) = %v, want ErrInvalidName", s, err)
		}
	}
}

func TestValidateShardCount(t *testing.T) {
	for _, s := range []uint32{1, 2, 1024} {
		if err := ValidateShardCount(s); err != nil {
			t.Errorf("ValidateShardCount(%d) = %v", s, err)
		}
	}
	for _, s := range []uint32{0, 1025, 1 << 31} {
		if err := ValidateShardCount(s); !errors.Is(err, ErrInvalidShardCount) {
			t.Errorf("ValidateShardCount(%d) = %v, want ErrInvalidShardCount", s, err)
		}
	}
}

// Golden vectors computed independently (Python hashlib): first 8 bytes of
// SHA-256(name) as a big-endian integer, then modulo S. They are permanent:
// a change here means the shard function changed, which breaks every volume.
func TestShardGolden(t *testing.T) {
	cases := []struct {
		name string
		s    uint32
		want uint32
	}{
		{"a", 1, 0}, {"a", 4, 2}, {"a", 7, 4}, {"a", 16, 10}, {"a", 1024, 458},
		{"A", 4, 1}, {"A", 7, 3}, {"A", 16, 9}, {"A", 1024, 377},
		{"a/b", 4, 1}, {"a/b", 7, 6}, {"a/b", 16, 13}, {"a/b", 1024, 925},
		{"docs/2026-10-29T12:00:00Z/page", 7, 2}, {"docs/2026-10-29T12:00:00Z/page", 16, 4},
		{"docs/2026-10-29T12:00:00Z/page", 1024, 708},
		{strings.Repeat("x", 512), 7, 5}, {strings.Repeat("x", 512), 16, 8}, {strings.Repeat("x", 512), 1024, 824},
	}
	for _, c := range cases {
		got, err := Shard(c.name, c.s)
		if err != nil || got != c.want {
			t.Errorf("Shard(%.20q, %d) = %d, %v; want %d", c.name, c.s, got, err, c.want)
		}
	}
}

func TestShardZeroCount(t *testing.T) {
	if _, err := Shard("a", 0); !errors.Is(err, ErrInvalidShardCount) {
		t.Fatalf("got %v, want ErrInvalidShardCount", err)
	}
}

func FuzzValidate(f *testing.F) {
	for _, s := range []string{"", "a", "a/b", "../x", "a//b", "\xff", strings.Repeat("a", 600)} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Must never panic; an accepted name satisfies the stated rules.
		if ValidateName(s) == nil {
			if len(s) < 1 || len(s) > MaxNameLen || s[0] == '/' || s[len(s)-1] == '/' || strings.Contains(s, "//") {
				t.Fatalf("accepted bad name %q", s)
			}
			for _, seg := range strings.Split(s, "/") {
				if seg == "." || seg == ".." || seg == "" {
					t.Fatalf("accepted bad segment in %q", s)
				}
			}
			if sh, err := Shard(s, 16); err != nil || sh >= 16 {
				t.Fatalf("shard out of range: %d, %v", sh, err)
			}
		}
		if ValidateBucket(s) == nil && (s == "." || s == ".." || s[0] == '.' || s[0] == '-' || strings.ContainsAny(s, "/:")) {
			t.Fatalf("accepted bad bucket %q", s)
		}
	})
}
