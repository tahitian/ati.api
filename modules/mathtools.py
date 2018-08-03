import random

def draw_from_box(box):
    picked = None
    try:
        total = 0
        keys = box.keys()
        for key in keys:
            value = box[key]
            total += value
        index = random.randint(1, total)
        total = 0
        for key in keys:
            value = box[key]
            total += value
            if index <= total:
                picked = key
                break
    except Exception, e:
        pass
    return picked
