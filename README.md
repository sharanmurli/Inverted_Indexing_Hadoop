# Inverted Indexing with Hadoop

This project is part of a coursework assignment at USC, where the goal is to create an inverted index from multiple text files using Hadoop's MapReduce framework. The project involves generating both unigram and bigram indices.

## Table of Contents

- [Summary](#summary)
- [Features](#features)
- [Technologies Used](#technologies-used)

## Summary

This homework involves writing code to index words from multiple text files and output an inverted index. The input data is pre-cleaned but contains some tab characters, which need to be handled. Punctuation and numerals are replaced with spaces, and all words are converted to lowercase. The final output includes both unigram and bigram indices.

## Features

- **Unigram Indexing:** Generates an inverted index of individual words across multiple text files.
- **Bigram Indexing:** Generates an inverted index for selected bigrams across a subset of text files.
- **Data Processing:** Handles punctuation, numerals, and case normalization.
- **MapReduce Implementation:** Utilizes Hadoop's MapReduce framework for efficient data processing.
- **Responsive and Scalable:** Designed to handle large datasets efficiently.

## Technologies Used

- **Hadoop MapReduce:** For distributed processing of large datasets.
- **Java:** Primary programming language for implementing MapReduce tasks.
- **Repl.it:** Online IDE for code development and testing.
- **GitHub:** Version control and code management.

