#!/usr/bin/env bash

export SETTINGS="DevelopmentConfig"
py.test --cov application tests/ --cov-report=term --cov-report=html