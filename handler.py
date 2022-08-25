def handle(signals):
    while True:
        print(f'handle: {signals.get()}')
