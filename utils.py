import time

def my_print(iteration, text, past_time, use_duration=True, testing=False):
    current_time = time.time()
    print_val = int(current_time - past_time) if use_duration else current_time
    print(f"Iteration {iteration}: {text} (s): {print_val}")
    if testing:
        time.sleep(3)
    return current_time

