FROM rust:latest as builder

WORKDIR /home/build
RUN git clone https://github.com/johnbcodes/sqlite-es.git
WORKDIR /home/build/sqlite-es
