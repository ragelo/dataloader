package examples

import (
	"context"
	"fmt"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/handler"
)

func resolveUserById(p graphql.ResolveParams, user_id string) (*User, error) {
	return extractLoaders(p).UserDataLoader.Load(user_id)
}

var userFriendSchema = graphql.NewObject(graphql.ObjectConfig{
	Name: "UserFriend",
	Fields: graphql.Fields{
		"id": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				user, err := resolveUserById(p, p.Source.(string))
				if err != nil {
					return nil, err
				}
				return user.ID, nil
			},
		},
		"name": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				user, err := resolveUserById(p, p.Source.(string))
				if err != nil {
					return nil, err
				}
				return user.Name, nil
			},
		},
	},
})

var userSchema = graphql.NewObject(graphql.ObjectConfig{
	Name: "User",
	Fields: graphql.Fields{
		"id": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return p.Source.(*User).ID, nil
			},
		},
		"name": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return p.Source.(*User).Name, nil
			},
		},
		"friends": &graphql.Field{
			Type: graphql.NewList(userFriendSchema),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				user := p.Source.(*User)
				if user == nil || user.ID == "" {
					return nil, error(fmt.Errorf("User ID is required"))
				}
				// Load friends for the user - dataloader will batch results for multipe resolvers
				// called for multiple users
				return extractLoaders(p).UserFriendsDataLoader.Load(user.ID)
			},
		},
	},
})

var rootQuery = graphql.NewObject(graphql.ObjectConfig{
	Name: "RootQuery",
	Fields: graphql.Fields{
		"allUsers": &graphql.Field{
			Type: graphql.NewList(userSchema),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Load all users from the service once
				return extractServices(p).UserService.GetAllUsers(p.Context)
			},
		},
	},
})

type Loaders struct {
	UserDataLoader        *UserDataLoader
	UserFriendsDataLoader *UserFriendsDataLoader
}

func extractLoaders(p graphql.ResolveParams) *Loaders {
	loaders := p.Info.RootValue.(map[string]interface{})["loaders"]
	return loaders.(*Loaders)
}

type Services struct {
	UserService *UserService
}

func extractServices(p graphql.ResolveParams) *Services {
	services := p.Info.RootValue.(map[string]interface{})["services"]
	return services.(*Services)
}

func StartGraphQL() {
	var schema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: rootQuery,
	})

	if err != nil {
		panic(err)
	}

	h := handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: true,
		RootObjectFn: func(ctx context.Context, r *http.Request) map[string]interface{} {
			return map[string]interface{}{
				"services": &Services{
					UserService: NewUserService(),
				},
				"loaders": &Loaders{
					UserDataLoader:        NewUserDataLoader(ctx),
					UserFriendsDataLoader: NewUserFriendsDataLoader(ctx),
				},
			}
		},
		FormatErrorFn: func(err error) gqlerrors.FormattedError {
			fmt.Println(fmt.Errorf("error: %v", err))
			return gqlerrors.NewFormattedError(fmt.Sprintf("error: %v", err))
		},
	})

	http.Handle("/graphql", h)

	http.ListenAndServe(":8081", nil)
}
