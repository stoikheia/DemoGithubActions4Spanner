#!/bin/sh

gcloud spanner instances create test-instance --config=emulator-config --description="20221127" --nodes=1
