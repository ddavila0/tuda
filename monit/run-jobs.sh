#!/bin/bash
timestamp() {
      date +"%s"
}

./monicron.sh "${1}" &> "/logs/$(timestamp).txt" &
