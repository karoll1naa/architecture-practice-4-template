package main

import (
	"gopkg.in/check.v1"
	"testing"
	"time"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type BalancerSuite struct{}

var _ = check.Suite(&BalancerSuite{})

func (s *BalancerSuite) TestBalancer(c *check.C) {
	healthChecker := &HealthChecker{}
	healthChecker.healthyServers = []string{"server1:8080", "server2:8080", "server3:8080"}

	balancer := &Balancer{}
	balancer.healthChecker = healthChecker

	index1 := balancer.getServerIndexWithLowestLoad(map[string]int64{
		"server1:8080": 100,
		"server2:8080": 200,
		"server3:8080": 150,
	}, []string{"server1:8080", "server2:8080", "server3:8080"})

	index2 := balancer.getServerIndexWithLowestLoad(map[string]int64{
		"server1:8080": 300,
		"server2:8080": 200,
		"server3:8080": 250,
	}, []string{"server1:8080", "server2:8080", "server3:8080"})

	index3 := balancer.getServerIndexWithLowestLoad(map[string]int64{
		"server1:8080": 200,
		"server2:8080": 150,
		"server3:8080": 100,
	}, []string{"server1:8080", "server2:8080", "server3:8080"})

	c.Assert(index1, check.Equals, 0)
	c.Assert(index2, check.Equals, 1)
	c.Assert(index3, check.Equals, 2)
}

func (s *BalancerSuite) TestHealthChecker(c *check.C) {
	healthChecker := &HealthChecker{}
	healthChecker.health = func(s string) bool {
		if s == "1" {
			return false
		} else {
			return true
		}
	}

	healthChecker.serversPool = []string{"1", "2", "3"}
	healthChecker.healthyServers = []string{"4", "5", "6"}
	healthChecker.checkInterval = 1 * time.Second

	healthChecker.StartHealthCheck()

	time.Sleep(3 * time.Second)

	c.Assert(healthChecker.GetHealthyServers()[0], check.Equals, "2")
	c.Assert(healthChecker.GetHealthyServers()[1], check.Equals, "3")
	c.Assert(len(healthChecker.GetHealthyServers()), check.Equals, 2)
}

func (s *BalancerSuite) TestScheme(c *check.C) {
	*https = true
	c.Assert(scheme(), check.Equals, "https")

	*https = false
	c.Assert(scheme(), check.Equals, "http")
}
