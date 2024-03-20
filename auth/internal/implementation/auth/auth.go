package auth

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"time"
	jwt "github.com/golang-jwt/jwt/v5"
	pb "github.com/vinay-winai/gomicro/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type implementation struct {
	db *sql.DB
	pb.UnimplementedAuthserviceServer
}

func NewAuthImplementation(db *sql.DB) *implementation {
	return &implementation{db: db}
}

func (imp *implementation) GetToken(ctx context.Context, credentials *pb.Credentials) (*pb.Token, error) {
	type user struct {
		userID   string
		password string
	}

	var u user
	stmt, err := imp.db.Prepare("select user_id, password from user where user_id = ? and password=?")
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = stmt.QueryRow(credentials.GetUserName(), credentials.GetPassword()).Scan(&u.userID, &u.password)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	
	jwt,err := createJWT(u.userID)
	if err != nil {
		return nil, err
	}
	
	return &pb.Token{Jwt: jwt}, nil
}

func createJWT(userId string) (string, error) {
	key := []byte(os.Getenv("SIGNING_KEY"))
	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "auth-service",
		"sub": userId,
		"iat": now.Unix(),
		"exp": now.Add(time.Hour * 24).Unix(),
	})

	signedToken, err := token.SignedString(key)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return signedToken, nil
}

func (imp *implementation) ValidateToken(ctx context.Context, token *pb.Token) (*pb.User, error) {
	key := []byte(os.Getenv("SIGNING_KEY"))
	userId, err := validateJwt(token.Jwt, key)
	if err != nil {
		return nil, err
	}
	return &pb.User{UserId: userId}, nil
}

func validateJwt(token string, signingKey []byte) (string, error) {
	type MyClaims struct {
		jwt.RegisteredClaims
	}
	parsedToken, err := jwt.ParseWithClaims(token, &MyClaims{}, func(token *jwt.Token) (interface{}, error) {
		return signingKey, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return "", status.Error(codes.Unauthenticated, "token expired")
		} else {
			return "", status.Error(codes.Unauthenticated, "unauthenticated")
		}
	}
	claims, ok := parsedToken.Claims.(*MyClaims)
	if !ok {
		return "", status.Error(codes.Internal, "claims type assertion failed")
	}

	return claims.RegisteredClaims.Subject, nil
}