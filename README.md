# Bitcask Go

![Thumbnail From Whitepaper](./thumbnail.png)

Toy implementation of Bitcask in Golang using the intro [whitepaper](https://riak.com/assets/bitcask-intro.pdf)

## Motive

I found out this log based KV store while reading through [DDIA](https://dataintensive.net/), and I really wanted to dive a little more than what was mentioned in the textbook. As a result, I made this toy example, that admittedly is not the best and clearly doesn't support a lot of the key features at the moment.

## Included Features

Current features include `GET`, `PUT`, `DELETE`, `KEYS`, and `MERGE`. I also made it possible to initialize from a preexisting Bitcask directory.

## Missing Features

At the moment I have yet to implement any concurrency, which I feel is really important to do in the future. There are other functions as well that I have yet to implement, but I won't state them all here.