package main

import "embed"

//go:embed web/*
var dashboardAssets embed.FS
