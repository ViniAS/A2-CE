#!/bin/bash


gnome-terminal --working-directory=. -- bash -c "poetry shell && python3 src/webhook_queue.py; exec bash"

gnome-terminal --working-directory=. -- bash -c "poetry shell && python3 src/webhook.py; exec bash"

gnome-terminal --working-directory=. -- bash -c "poetry shell && python3 mock/web_gen.py; exec bash"


