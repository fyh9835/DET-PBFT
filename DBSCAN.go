package main

import (
	"math"
	"math/rand"
	"time"
)

type Point struct {
	X, Y      float64
	ClusterID int
	Visited   bool
}

func euclideanDistance(a, b Point) float64 {
	return math.Sqrt(math.Pow((a.X-b.X), 2) + math.Pow((a.Y-b.Y), 2))
}

func regionQuery(points []Point, i int, eps float64) []int {
	var neighbors []int
	for j, p := range points {
		if i != j && euclideanDistance(points[i], p) < eps {
			neighbors = append(neighbors, j)
		}
	}
	return neighbors
}

func expandCluster(points []Point, i, clusterID int, eps float64, minPts int) {
	neighbors := regionQuery(points, i, eps)

	if len(neighbors) < minPts {
		points[i].ClusterID = -1 // mark as noise
	} else {
		points[i].ClusterID = clusterID
		for _, neighborID := range neighbors {
			if !points[neighborID].Visited {
				points[neighborID].Visited = true
				expandCluster(points, neighborID, clusterID, eps, minPts)
			}
			if points[neighborID].ClusterID == 0 || points[neighborID].ClusterID == -1 {
				points[neighborID].ClusterID = clusterID
			}
		}
	}
}

func dbscan(points []Point, eps float64, minPts int) {
	clusterID := 1

	for i := 0; i < len(points); i++ {
		if !points[i].Visited {
			points[i].Visited = true
			expandCluster(points, i, clusterID, eps, minPts)
			if points[i].ClusterID != -1 {
				clusterID++
			}
		}
	}
}

func generateRandomPoints(n int) []Point {
	rand.Seed(time.Now().UnixNano())
	var points []Point

	for i := 0; i < n; i++ {
		points = append(points, Point{X: rand.Float64() * 999, Y: rand.Float64() * 999})
	}

	return points
}
