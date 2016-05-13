# Example Voting App

This example is an adaptation of the Docker 3th birthday voting demo
app originally found in this repo:

	https://github.com/docker/docker-birthday-3

It's adapter to run on Kubernetes 1.2 and up.

##Architecture

* A Python webapp which lets you vote between two options
* A Redis queue which collects new votes
* A Java worker which consumes votes and stores them in…
* A Postgres database backed by a Docker volume
* A Node.js webapp which shows the results of the voting in real time

##Pre-Requirement

### Docker base images

At the time this demo is made most of the current alpine based images
are not working well on Kubernetes because they have a disfunctional
dns lookup machanism. This is fixed on the EDGE version of Docker.

You can build your own images with the adapted docker files in the
images folder. They will be tagged with a specific label that are used
in the application docker files.

So first run the build.sh file in the images directory and check if you
have 4 new tagged images (prefixed with k8s-).

### Google Container Engine

This examples are created for GKE and demonstrates some of the Google
Cloud Platform integration. You can adapt them for your own environment.

##

