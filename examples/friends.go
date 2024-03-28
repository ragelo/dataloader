package examples

import (
	"context"

	dl "github.com/ragelo/dataloader"
)

type UserFriendsService struct {
	friends map[string]*[]string
}

var (
	_friends = map[string]*[]string{
		"1": {"2", "3"},
		"2": {"1", "3"},
		"3": {"1", "2"},
	}
)

func NewUserFriendsService() *UserFriendsService {
	return &UserFriendsService{
		friends: _friends,
	}
}

func (s *UserFriendsService) GetUserFriendsMap(ctx context.Context, user_ids *[]string) (map[string]*[]string, error) {
	result := make(map[string]*[]string)
	for _, user_id := range *user_ids {
		if friends, ok := s.friends[user_id]; ok {
			result[user_id] = friends
		} else {
			result[user_id] = nil
		}
	}
	return result, nil
}

type UserFriendsLoaderConfig struct {
	dl.IDataLoaderConfig[string, []string]

	userFriendsService *UserFriendsService
}

func (c *UserFriendsLoaderConfig) BatchLoad(ctx context.Context, keys *[]string) (map[string]*[]string, error) {
	return c.userFriendsService.GetUserFriendsMap(ctx, keys)
}

func NewUserFriendsLoader(ctx context.Context) *dl.DataLoader[string, []string] {
	config := &UserFriendsLoaderConfig{
		userFriendsService: NewUserFriendsService(),
	}
	return dl.NewDataLoader[string, []string](ctx, config, 10 /* max 10 items per match */, 5 /* max 5ms batching window */)
}
