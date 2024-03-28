package main

import (
	"context"

	"github.com/gin-gonic/gin"

	dl "github.com/ragelo/dataloader"
)

var (
	users = map[string]*User{
		"1": {ID: "1", Name: "User 1"},
		"2": {ID: "2", Name: "User 2"},
		"3": {ID: "3", Name: "User 3"},
	}
	user_friends = map[string]*[]string{
		"1": {"2", "3"},
		"2": {"1", "3"},
		"3": {"1", "2"},
	}
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type UserView struct {
	User
	Friends []*User `json:"friends"`
}

type UserDataLoaderConfig struct {
	dl.IDataLoaderConfig[string, User]
}

func (c *UserDataLoaderConfig) BatchLoad(ctx context.Context, keys *[]string) (map[string]*User, error) {
	result := make(map[string]*User)
	for _, key := range *keys {
		if user, ok := users[key]; ok {
			result[key] = user
		} else {
			result[key] = nil
		}
	}
	return result, nil
}

func NewUserDataLoader(ctx context.Context) *dl.DataLoader[string, User] {
	config := &UserDataLoaderConfig{}
	return dl.NewDataLoader[string, User](ctx, config, 10 /* max 10 items per match */, 5 /* max 5ms batching window */)
}

type UserFriendsLoaderConfig struct {
	dl.IDataLoaderConfig[string, []string]
}

func (c *UserFriendsLoaderConfig) BatchLoad(ctx context.Context, keys *[]string) (map[string]*[]string, error) {
	result := make(map[string]*[]string)
	for _, key := range *keys {
		if friends, ok := user_friends[key]; ok {
			result[key] = friends
		} else {
			result[key] = nil
		}
	}
	return result, nil
}

func NewUserFriendsLoader(ctx context.Context) *dl.DataLoader[string, []string] {
	config := &UserFriendsLoaderConfig{}
	return dl.NewDataLoader[string, []string](ctx, config, 10 /* max 10 items per match */, 5 /* max 5ms batching window */)
}

func getUsers(c *gin.Context) {
	ctx := context.Background()
	userDataLoader := NewUserDataLoader(ctx)
	userFriendsListDataLoader := NewUserFriendsLoader(ctx)
	ch := make(chan *UserView)

	for userId := range users {
		go func(user *User) {
			// Fetch user friend IDs from service 1
			friendIds, err := userFriendsListDataLoader.Load(user.ID)
			if err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}

			// Fetch user details for each friend from service 2
			friends, err := userDataLoader.LoadMany(friendIds)
			if err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}

			ch <- &UserView{
				User:    *user,
				Friends: friends,
			}
		}(users[userId])
	}
	results := make([]*UserView, 0)

	for {
		select {
		case userView := <-ch:
			results = append(results, userView)
			if len(results) == len(users) {
				c.JSON(200, results)
				return
			}
		}
	}
}

func main() {
	router := gin.Default()
	router.GET("/users", getUsers)

	router.Run("localhost:8080")
}
