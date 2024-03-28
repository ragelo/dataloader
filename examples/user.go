package examples

import (
	"context"

	dl "github.com/ragelo/dataloader"
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type UserService struct {
	users map[string]*User
}

var (
	_users = map[string]*User{
		"1": {ID: "1", Name: "User 1"},
		"2": {ID: "2", Name: "User 2"},
		"3": {ID: "3", Name: "User 3"},
	}
)

func NewUserService() *UserService {
	return &UserService{
		users: _users,
	}
}

func (s *UserService) GetUsersMap(ctx context.Context, ids *[]string) (map[string]*User, error) {
	result := make(map[string]*User)
	for _, key := range *ids {
		if user, ok := s.users[key]; ok {
			result[key] = user
		} else {
			result[key] = nil
		}
	}
	return result, nil
}

func (s *UserService) GetAllUsers(ctx context.Context) ([]*User, error) {
	result := make([]*User, 0)
	for _, user := range s.users {
		result = append(result, user)
	}
	return result, nil
}

type UserBatchLoader struct {
	userService *UserService
}

func (c *UserBatchLoader) BatchLoad(ctx context.Context, keys *[]string) (map[string]*User, error) {
	return c.userService.GetUsersMap(ctx, keys)
}

type UserDataLoader = dl.DataLoader[string, User]

func NewUserDataLoader(ctx context.Context) *UserDataLoader {
	batchLoader := &UserBatchLoader{
		userService: NewUserService(),
	}
	return dl.NewDataLoader[string, User](ctx, batchLoader, 10 /* max 10 items per match */, 5 /* max 5ms batching window */)
}
