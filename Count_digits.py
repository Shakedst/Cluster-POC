def count(text):
    arr = [0]*10
    for c in text:
        arr[int(c)] += 1
    return arr