#!/bin/bash
timestamp() {
      date +"%s"
}

./populate.sh &> "/logs/$(timestamp).txt" &
