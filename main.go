// Copyright 2019 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/fsnotify/fsnotify"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/google/pprof/profile"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	pb "google.golang.org/genproto/googleapis/devtools/cloudprofiler/v2"
)

var (
	client pb.ProfilerServiceClient

	project   string
	zone      string
	target    string
	namespace string

	input        string
	keepTime     bool
	offline      bool
	durationSecs int

	latestProfile *pb.Profile
)

const (
	apiAddr = "cloudprofiler.googleapis.com:443"
	scope   = "https://www.googleapis.com/auth/monitoring.write"
)

func main() {
	ctx := context.Background()
	flag.StringVar(&project, "project", "", "")
	flag.StringVar(&zone, "zone", "", "")
	flag.StringVar(&target, "target", "", "")
	flag.StringVar(&namespace, "namespace", "", "")
	flag.StringVar(&input, "i", "pprof.out", "")
	flag.BoolVar(&keepTime, "keep-time", false, "")
	flag.BoolVar(&offline, "offline", false, "")
	flag.IntVar(&durationSecs, "duration", 15, "")
	flag.Usage = usageAndExit
	flag.Parse()

	// TODO(jbd): Automatically detect input. Don't convert if pprof.

	if project == "" {
		id, err := metadata.ProjectID()
		if err != nil {
			log.Fatalf("Cannot resolve the GCP project from the metadata server: %v", err)
		}
		project = id
	}
	if zone == "" {
		// Ignore error. If we cannot resolve the instance name,
		// it would be too aggressive to fatal exit.
		zone, _ = metadata.Zone()
	}

	if target == "" {
		target = input
	}

	conn, err := gtransport.Dial(ctx,
		option.WithEndpoint(apiAddr),
		option.WithScopes(scope))
	if err != nil {
		log.Fatal(err)
	}
	client = pb.NewProfilerServiceClient(conn)

	if !offline {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Fatal(err)
		}
		defer watcher.Close()
		done := make(chan bool)

		for {
			if _, err := os.Stat(input); os.IsNotExist(err) {
				fmt.Printf("Waiting for file '%v' to be created.\n", input)
			} else if err != nil {
				fmt.Printf("Unexpected error while waiting for '%v' to be created.\n", input)
			} else {
				break
			}
			time.Sleep(3 * time.Second)
		}

		payload, err := ioutil.ReadFile(input)
		if err != nil {
			log.Fatalf("unable to read file %v: %v\n", input, err)
			return
		}
		latestProfile, err = create(ctx, payload)
		if err != nil {
			log.Fatalf("unable to create profile (does it already exist?): %v\n", err)
			return
		}

		go func() {
			for {
				select {
				case event := <-watcher.Events:
					if (event.Op&fsnotify.Create != 0) || (event.Op&fsnotify.Write != 0) {
						payload, err := ioutil.ReadFile(input)
						if err != nil {
							log.Fatalf("unable to read file %v: %v\n", input, err)
							continue
						}
						latestProfile, err = create(ctx, payload)
						if err != nil {
							log.Fatalf("unable to update profile: %v\n", err)
							continue
						}
						_, err = update(ctx, payload)
						if err != nil {
							log.Fatalf("unable to update profile: %v\n", err)
							continue
						}
					}
				case err := <-watcher.Errors:
					log.Fatalf("error watching %v with fsnotify: %v\n", input, err)
				}
			}
		}()

		if err := watcher.Add(input); err != nil {
			log.Fatalf("unable to set up fsnotify watcher for file %v: %v\n", input, err)
			return
		}

		<-done

		log.Printf("shutting down pprof-upload agent\n")

		if err := watcher.Close(); err != nil {
			log.Fatalf("unable to set up fsnotify watcher for file %v: %v\n", input, err)
			return
		}
	} else {
		payload, err := ioutil.ReadFile(input)
		if err != nil {
			log.Fatalf("unable to read file %v: %v\n", input, err)
			return
		}
		if err := upload(ctx, payload); err != nil {
			log.Fatalf("Cannot upload to Stackdriver Profiler: %v\n", err)
		}
		fmt.Printf("https://console.cloud.google.com/profiler/%s;type=%s?project=%s\n", url.PathEscape(target), pb.ProfileType_CPU, project)
	}
}

func create(ctx context.Context, payload []byte) (*pb.Profile, error) {
	deployment := &pb.Deployment{
		ProjectId: project,
		Target:    target,
		Labels: map[string]string{
			"zone":    zone,
			"version": namespace,
		},
	}
	req := &pb.CreateProfileRequest{
		Parent:      "projects/" + project,
		Deployment:  deployment,
		ProfileType: []pb.ProfileType{pb.ProfileType_CPU},
	}
	return client.CreateProfile(ctx, req)
}

func update(ctx context.Context, payload []byte) (*pb.Profile, error) {
	newProfile := &pb.Profile{
		Name:         latestProfile.Name,
		ProfileType:  latestProfile.ProfileType,
		Deployment:   latestProfile.Deployment,
		Duration:     &duration.Duration{Seconds: int64(durationSecs), Nanos: 0},
		ProfileBytes: payload,
		Labels:       latestProfile.Labels,
	}
	req := &pb.UpdateProfileRequest{
		Profile: newProfile,
	}
	return client.UpdateProfile(ctx, req)
}

func upload(ctx context.Context, payload []byte) error {
	if !keepTime {
		var err error
		payload, err = resetTime(payload)
		if err != nil {
			log.Printf("Cannot reset the profile's time: %v", err)
		}
	}

	req := &pb.CreateOfflineProfileRequest{
		Parent: "projects/" + project,
		Profile: &pb.Profile{
			ProfileType: pb.ProfileType_CPU,
			Deployment: &pb.Deployment{
				ProjectId: project,
				Target:    target,
				Labels: map[string]string{
					"zone":    zone,
					"version": namespace,
				},
			},
			ProfileBytes: payload,
		},
	}
	_, err := client.CreateOfflineProfile(ctx, req)
	return err
}

// TODO(jbd): Make it optional.
func resetTime(pprofBytes []byte) ([]byte, error) {
	p, err := profile.ParseData(pprofBytes)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse the profile: %v", err)
	}
	p.TimeNanos = time.Now().UnixNano()

	var buf bytes.Buffer
	if err := p.Write(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// TODO(jbd): Check binary dependencies and install if not available.

const usageText = `pprof-upload [-i pprof.out]

Other options:
-project    Google Cloud project name, tries to automatically
            resolve if none is set.
-zone       Google Cloud zone, tries to automatically resolve if
		    none is set.
-target     Target profile name to upload data to.
-keep-time  When set, keeps the original time info from the profile file.
			Due to data retention limits, Stackdriver Profiler won't
            show data older than 30 days. By default, false.
-offline    When set, polls the file specified with -i for changes using fsnotify,
	    updating an online profile accordingly. When not set, sends a one-off
	    offline profile creation request with the contents of the profile
	    specified by -i.
-namespace  If set, adds the "version" label to the corresponding profile. This
	    is typically used when upload profiles that are associated with a specific
	    customer namespace.
-duration   Set the duration (in seconds) that each profile accounts for.
`

func usageAndExit() {
	fmt.Println(usageText)
	os.Exit(1)
}
