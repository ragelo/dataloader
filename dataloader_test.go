package dataloader

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

type user struct {
	ID   string
	Name string
}

type userDataLoaderConfig struct {
	IDataLoaderConfig[string, user]

	users map[string]*user

	calls int
}

func (c *userDataLoaderConfig) BatchLoad(ctx context.Context, keys *[]string) (map[string]*user, error) {
	users := make(map[string]*user)
	for _, key := range *keys {
		if user, ok := c.users[key]; ok {
			users[key] = user
		} else {
			users[key] = nil
		}
	}
	c.calls += 1
	return users, nil
}

func generateTestUsers() map[string]*user {
	users := make(map[string]*user)
	// Generate test users
	for i := 0; i < 20; i++ {
		u := &user{
			ID:   fmt.Sprintf("%d", i),
			Name: fmt.Sprintf("User %d", i),
		}
		users[u.ID] = u
	}

	return users
}

func TestNewDataLoader(t *testing.T) {
	// Create a new data loader.
	users := generateTestUsers()
	c := &userDataLoaderConfig{users: users, calls: 0}
	dataLoader := NewDataLoader[string, user](context.Background(), c, 2, 20)

	// Load a user by ID (sync)
	loadedUser, err := dataLoader.Load("0")
	if err != nil {
		t.Errorf("Expected error to be nil, but got %v", err)
	}
	if loadedUser != users["0"] {
		t.Errorf("Expected user to be %v, but got %v", users["0"], loadedUser)
	}
	if c.calls != 1 {
		t.Errorf("Expected calls to be 1, but got %v", c.calls)
	}
	c.calls = 0

	// Load a user by ID (async)
	wg := sync.WaitGroup{}
	for i := 1; i < 11; i++ {
		go func(id string) {
			loadedUser, err := dataLoader.Load(id)
			if err != nil {
				t.Errorf("Expected error to be nil, but got %v", err)
			}
			if loadedUser != users[id] {
				t.Errorf("Expected user to be %v, but got %v", users[id], loadedUser)
			}
			wg.Done()
		}(fmt.Sprintf("%d", i))
		wg.Add(1)
	}
	wg.Wait()

	if c.calls != 5 {
		t.Errorf("Expected calls to be 5, but got %v", c.calls)
	}
	c.calls = 0

	loadedUser, err = dataLoader.Load("55")
	if err != nil {
		t.Errorf("Expected error to be nil, but got %v", err)
	}
	if loadedUser != nil {
		t.Errorf("Expected user to be nil, but got %v", loadedUser)
	}

	if c.calls != 1 {
		t.Errorf("Expected calls to be 1, but got %v", c.calls)
	}
	c.calls = 0

	loadedUsers, err := dataLoader.LoadMany(&[]string{"12", "13", "14"})
	expectedUsers := []*user{users["12"], users["13"], users["14"]}
	if err != nil {
		t.Errorf("Expected error to be nil, but got %v", err)
	}
	if !reflect.DeepEqual(loadedUsers, expectedUsers) {
		t.Errorf("Expected users to be %v, but got %v", expectedUsers, loadedUsers)
	}

	if c.calls != 2 {
		t.Errorf("Expected calls to be 2, but got %v", c.calls)
	}
	c.calls = 0
}
