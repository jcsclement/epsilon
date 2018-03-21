# Databricks notebook source
topicsDF = spark.read.format('csv').load('/mnt/media-breakdowns-processed/topics.csv', header = True)
display(topicsDF)