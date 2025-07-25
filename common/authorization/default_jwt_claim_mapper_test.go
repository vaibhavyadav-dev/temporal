package authorization

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

type errorTestOptions int16

const (
	errorTestOptionNoKID = errorTestOptions(1 << iota)
	errorTestOptionNoSubject
	errorTestOptionNoAlgorithm
	errorTestOptionNoError = errorTestOptions(0)
)

type keyAlgorithm int8

const (
	RSA keyAlgorithm = iota
	ECDSA
)

const (
	testSubject      = "test-user"
	defaultNamespace = "default"
)

var (
	permissionsAdmin              = []string{primitives.SystemLocalNamespace + ":admin", "default:read"}
	permissionsReaderWriterWorker = []string{"default:read", "default:write", "default:worker"}
)

type (
	defaultClaimMapperSuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		tokenGenerator *tokenGenerator
		claimMapper    ClaimMapper
		config         *config.Authorization
		logger         log.Logger
	}
)

func TestDefaultClaimMapperSuite(t *testing.T) {
	s := new(defaultClaimMapperSuite)
	suite.Run(t, s)
}
func (s *defaultClaimMapperSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.tokenGenerator = newTokenGenerator()
	s.config = &config.Authorization{}
	s.logger = log.NewNoopLogger()
	s.claimMapper = NewDefaultJWTClaimMapper(s.tokenGenerator, s.config, s.logger)
}
func (s *defaultClaimMapperSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *defaultClaimMapperSuite) TestTokenGeneratorRSA() {
	s.testTokenGenerator(RSA)
}
func (s *defaultClaimMapperSuite) TestTokenGeneratorECDSA() {
	s.testTokenGenerator(ECDSA)
}
func (s *defaultClaimMapperSuite) testTokenGenerator(alg keyAlgorithm) {
	tokenString, err := s.tokenGenerator.generateToken(alg,
		testSubject, permissionsAdmin, errorTestOptionNoError)
	s.NoError(err)
	claims, err := parseJWT(tokenString, s.tokenGenerator)
	s.NoError(err)
	s.Equal(testSubject, claims["sub"])
}

func (s *defaultClaimMapperSuite) TestTokenWithNoSubject() {
	tokenString, err := s.tokenGenerator.generateRSAToken(
		testSubject, permissionsAdmin, errorTestOptionNoSubject)
	s.NoError(err)
	claims, err := parseJWT(tokenString, s.tokenGenerator)
	s.NoError(err)
	subject := claims["sub"]
	s.Nil(subject)
}
func (s *defaultClaimMapperSuite) TestTokenWithNoKID() {
	tokenString, err := s.tokenGenerator.generateRSAToken(
		testSubject, permissionsAdmin, errorTestOptionNoKID)
	s.NoError(err)
	_, err = parseJWT(tokenString, s.tokenGenerator)
	s.Error(err, "malformed token - no \"kid\" header")
}
func (s *defaultClaimMapperSuite) TestTokenWithNoAlgorithm() {
	tokenString, err := s.tokenGenerator.generateRSAToken(
		testSubject, permissionsAdmin, errorTestOptionNoAlgorithm)
	s.NoError(err)
	_, err = parseJWT(tokenString, s.tokenGenerator)
	s.Error(err, "signing method (alg) is unspecified.")
}

func (s *defaultClaimMapperSuite) TestTokenWithAdminPermissionsRSA() {
	s.testTokenWithAdminPermissions(RSA)
}
func (s *defaultClaimMapperSuite) TestTokenWithAdminPermissionsECDSA() {
	s.testTokenWithAdminPermissions(ECDSA)
}
func (s *defaultClaimMapperSuite) testTokenWithAdminPermissions(alg keyAlgorithm) {
	tokenString, err := s.tokenGenerator.generateToken(alg,
		testSubject, permissionsAdmin, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
		"",
	}
	claims, err := s.claimMapper.GetClaims(authInfo)
	s.NoError(err)
	s.Equal(testSubject, claims.Subject)
	s.Equal(RoleAdmin, claims.System)
	s.Equal(1, len(claims.Namespaces))
	defaultRole := claims.Namespaces[defaultNamespace]
	s.Equal(RoleReader, defaultRole)
}

func (s *defaultClaimMapperSuite) TestNamespacePermissionCaseSensitive() {
	tokenString, err := s.tokenGenerator.generateToken(RSA,
		testSubject, []string{"Temporal-system:admin", "Foo:read"}, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
		"",
	}
	claims, err := s.claimMapper.GetClaims(authInfo)
	s.NoError(err)
	s.Equal(testSubject, claims.Subject)
	s.Equal(RoleUndefined, claims.System) // no system role
	s.Equal(2, len(claims.Namespaces))
	// claims contain namespace role for 'Foo', not for 'foo'.
	s.Equal(RoleReader, claims.Namespaces["Foo"])
	s.Equal(RoleAdmin, claims.Namespaces["Temporal-system"])
}

func (s *defaultClaimMapperSuite) TestTokenWithReaderWriterWorkerPermissionsRSA() {
	s.testTokenWithReaderWriterWorkerPermissions(RSA)
}
func (s *defaultClaimMapperSuite) TestTokenWithReaderWriterWorkerPermissionsECDSA() {
	s.testTokenWithReaderWriterWorkerPermissions(ECDSA)
}
func (s *defaultClaimMapperSuite) testTokenWithReaderWriterWorkerPermissions(alg keyAlgorithm) {
	tokenString, err := s.tokenGenerator.generateToken(
		alg, testSubject, permissionsReaderWriterWorker, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
		"test-audience",
	}
	claims, err := s.claimMapper.GetClaims(authInfo)
	s.NoError(err)
	s.Equal(testSubject, claims.Subject)
	s.Equal(RoleUndefined, claims.System)
	s.Equal(1, len(claims.Namespaces))
	defaultRole := claims.Namespaces[defaultNamespace]
	s.Equal(RoleReader|RoleWriter|RoleWorker, defaultRole)
}

func (s *defaultClaimMapperSuite) TestTokenWithReaderWriterWorkerPermissionsRegex() {
	permissions := []string{"read:default", "write:default", "worker:default"}
	tokenString, err := s.tokenGenerator.generateToken(RSA, testSubject, permissions, errorTestOptionNoError)
	s.NoError(err)
	authConfig := &config.Authorization{PermissionsRegex: `(?P<role>\w+):(?P<namespace>\w+)`}
	claimMapper := NewDefaultJWTClaimMapper(s.tokenGenerator, authConfig, log.NewNoopLogger())
	s.NotNil(claimMapper)
	authInfo := &AuthInfo{AuthToken: AddBearer(tokenString), Audience: "test-audience"}
	claims, err := claimMapper.GetClaims(authInfo)
	s.NoError(err)
	s.Equal(testSubject, claims.Subject)
	s.Equal(RoleUndefined, claims.System)
	s.Equal(1, len(claims.Namespaces))
	defaultRole := claims.Namespaces[defaultNamespace]
	s.Equal(RoleReader|RoleWriter|RoleWorker, defaultRole)
}

func (s *defaultClaimMapperSuite) TestGetClaimMapperFromConfigNoop() {
	s.testGetClaimMapperFromConfig("", true, reflect.TypeOf(&noopClaimMapper{}))
}
func (s *defaultClaimMapperSuite) TestGetClaimMapperFromConfigDefault() {
	s.testGetClaimMapperFromConfig("default", true, reflect.TypeOf(&defaultJWTClaimMapper{}))
}

func (s *defaultClaimMapperSuite) TestGetClaimMapperFromConfigUnknown() {
	s.testGetClaimMapperFromConfig("foo", false, nil)
}

func (s *defaultClaimMapperSuite) TestGetClaimMapperWithPermissionsRegexInvalidRegex() {
	pattern := `(?P<namespace\w+):(?P<role>\w+)`
	mapper := NewDefaultJWTClaimMapper(nil, &config.Authorization{PermissionsRegex: pattern}, log.NewNoopLogger()).(*defaultJWTClaimMapper)
	s.Nil(mapper.permissionsRegex)
	s.Zero(mapper.matchNamespaceIndex)
	s.Zero(mapper.matchRoleIndex)
}

func (s *defaultClaimMapperSuite) TestGetClaimMapperWithPermissionsRegexMissingNamespaceGroup() {
	pattern := `(?P<role>\w+):(\w+)`
	mapper := NewDefaultJWTClaimMapper(
		nil, &config.Authorization{PermissionsRegex: pattern}, log.NewNoopLogger(),
	).(*defaultJWTClaimMapper)
	s.Nil(mapper.permissionsRegex)
}

func (s *defaultClaimMapperSuite) TestGetClaimMapperWithPermissionsRegexMissingRoleGroup() {
	pattern := `(?P<namespace>\w+):(\w+)`
	mapper := NewDefaultJWTClaimMapper(
		nil, &config.Authorization{PermissionsRegex: pattern}, log.NewNoopLogger(),
	).(*defaultJWTClaimMapper)
	s.Nil(mapper.permissionsRegex)
}

func (s *defaultClaimMapperSuite) TestGetClaimMapperWithPermissionsRegex() {
	authConfig := &config.Authorization{PermissionsRegex: `(?P<role>\w+):(?P<namespace>\w+)`}
	mapper := NewDefaultJWTClaimMapper(nil, authConfig, nil).(*defaultJWTClaimMapper)
	s.NotNil(mapper.permissionsRegex)
	s.NotZero(mapper.matchNamespaceIndex)
	s.NotZero(mapper.matchRoleIndex)
}

func (s *defaultClaimMapperSuite) TestTokenWithAdminPermissionsRegex() {
	permissions := []string{"admin:" + primitives.SystemLocalNamespace, "read:default"}
	pattern := `(?P<role>[\w-]+):(?P<namespace>[\w-]+)`
	tokenString, err := s.tokenGenerator.generateToken(RSA, testSubject, permissions, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{AuthToken: AddBearer(tokenString)}
	authConfig := &config.Authorization{PermissionsRegex: pattern}
	claimMapper := NewDefaultJWTClaimMapper(s.tokenGenerator, authConfig, nil)
	claims, err := claimMapper.GetClaims(authInfo)
	s.NoError(err)
	s.Equal(testSubject, claims.Subject)
	s.Equal(RoleAdmin, claims.System)
	s.Equal(1, len(claims.Namespaces))
	defaultRole := claims.Namespaces[defaultNamespace]
	s.Equal(RoleReader, defaultRole)
}

func (s *defaultClaimMapperSuite) TestWrongAudience() {
	tokenString, err := s.tokenGenerator.generateRSAToken(testSubject, permissionsAdmin, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
		"foo",
	}
	_, err = s.claimMapper.GetClaims(authInfo)
	s.Error(err)
}

func (s *defaultClaimMapperSuite) TestCorrectAudience() {
	tokenString, err := s.tokenGenerator.generateRSAToken(testSubject, permissionsAdmin, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
		"test-audience",
	}
	_, err = s.claimMapper.GetClaims(authInfo)
	s.NoError(err)
}

func (s *defaultClaimMapperSuite) TestIgnoreAudience() {
	tokenString, err := s.tokenGenerator.generateRSAToken(testSubject, permissionsAdmin, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
		"",
	}
	_, err = s.claimMapper.GetClaims(authInfo)
	s.NoError(err)
}

func (s *defaultClaimMapperSuite) testGetClaimMapperFromConfig(name string, valid bool, cmType reflect.Type) {

	cfg := config.Authorization{}
	cfg.ClaimMapper = name
	cm, err := GetClaimMapperFromConfig(&cfg, s.logger)
	if valid {
		s.NoError(err)
		s.NotNil(cm)
		t := reflect.TypeOf(cm)
		s.True(t == cmType)
	} else {
		s.Error(err)
		s.Nil(cm)
	}
}

func AddBearer(token string) string {
	return "Bearer " + token
}

type (
	tokenGenerator struct {
		rsaPrivateKey   *rsa.PrivateKey
		rsaPublicKey    *rsa.PublicKey
		ecdsaPrivateKey *ecdsa.PrivateKey
		ecdsaPublicKey  *ecdsa.PublicKey
	}
)

var _ TokenKeyProvider = (*tokenGenerator)(nil)

func newTokenGenerator() *tokenGenerator {

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil
	}
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil
	}

	return &tokenGenerator{
		rsaPrivateKey:   rsaKey,
		rsaPublicKey:    &rsaKey.PublicKey,
		ecdsaPrivateKey: ecdsaKey,
		ecdsaPublicKey:  &ecdsaKey.PublicKey,
	}
}

type (
	CustomClaims struct {
		Permissions []string `json:"permissions"`
		jwt.RegisteredClaims
	}
)

func (CustomClaims) Valid() error {
	return nil
}

func (tg *tokenGenerator) generateRSAToken(subject string, permissions []string, options errorTestOptions) (string, error) {
	return tg.generateToken(RSA, subject, permissions, options)
}

func (tg *tokenGenerator) generateToken(alg keyAlgorithm, subject string, permissions []string, options errorTestOptions) (string, error) {
	claims := CustomClaims{
		permissions,
		jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			Issuer:    "test",
			Audience:  []string{"test-audience"},
		},
	}
	if options&errorTestOptionNoSubject == 0 {
		claims.Subject = subject
	}

	var token *jwt.Token
	switch alg {
	case RSA:
		token = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	case ECDSA:
		token = jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	default:
		return "", fmt.Errorf("unsupported algorithm")
	}

	if options&errorTestOptionNoKID == 0 {
		token.Header["kid"] = "test-key"
	}
	if options&errorTestOptionNoAlgorithm > 0 {
		delete(token.Header, "alg")
	}

	switch alg {
	case RSA:
		return token.SignedString(tg.rsaPrivateKey)
	case ECDSA:
		return token.SignedString(tg.ecdsaPrivateKey)
	}
	return "", fmt.Errorf("unexpected condition")
}

func (tg *tokenGenerator) EcdsaKey(alg string, kid string) (*ecdsa.PublicKey, error) {
	return tg.ecdsaPublicKey, nil
}
func (tg *tokenGenerator) HmacKey(alg string, kid string) ([]byte, error) {
	return nil, fmt.Errorf("unsupported key type HMAC for: %s", alg)
}
func (tg *tokenGenerator) RsaKey(alg string, kid string) (*rsa.PublicKey, error) {
	return tg.rsaPublicKey, nil
}
func (tg *tokenGenerator) SupportedMethods() []string {
	return []string{jwt.SigningMethodRS256.Name, jwt.SigningMethodES256.Name}
}
func (tg *tokenGenerator) Close() {
}
