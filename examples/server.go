package examples

import (
	"context"

	"github.com/gin-gonic/gin"
)

type UserView struct {
	User
	Friends []*User `json:"friends"`
}

func getUsers(c *gin.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	usersService := NewUserService()
	userDataLoader := NewUserDataLoader(ctx)
	userFriendsListDataLoader := NewUserFriendsLoader(ctx)

	users, err := usersService.GetAllUsers(ctx)

	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

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

	for range users {
		results = append(results, <-ch)
	}

	c.JSON(200, results)
}

func Start() {
	router := gin.Default()
	router.GET("/users", getUsers)

	router.Run("localhost:8080")
}
