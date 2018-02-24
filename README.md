This repository contains the starter code for project 2 (14-736).
These instructions assume you have set your `GOPATH` to point to the repository's
root `raft-go/` directory.

## Starter Code

The starter code for this project is organized roughly as follows:

```
  raft/                            Raft implementation, tests and test helpers

  labrpc/                          RPC library that must be used for implementing Raft
    
```

## Instructions

##### How to Write Go Code

If at any point you have any trouble with building, installing, or testing your code, the article
titled [How to Write Go Code](http://golang.org/doc/code.html) is a great resource for understanding
how Go workspaces are built and organized. You might also find the documentation for the
[`go` command](http://golang.org/cmd/go/) to be helpful. As always, feel free to post your questions
on Piazza.

### Executing the official tests

#### 1. Checkpoint

To run the checkpoint tests, run the following from the raft/ folder

```bash
go test -run 3A
```

#### 2. Full test

To execute all the tests, run the following from the raft/ folder

```bash
go test
```

