FROM rust:1.40 as builder

WORKDIR /home/build
RUN git clone https://github.com/serverlesstechnology/postgres.git
WORKDIR /home/build/cqrs

