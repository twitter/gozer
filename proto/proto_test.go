package proto

import (
	"testing"
	"time"
)

func TestSHA(t *testing.T) {
	if len(GitSHA()) != 40 {
		t.Errorf("len(GitSHA()) == %d, %d != 40", len(GitSHA()), len(GitSHA()))
	}
}

func TestTime(t *testing.T) {
	if GitTime().After(time.Now()) {
		t.Errorf("GitTime() is %v, which is in the future", GitTime())
	}
}
