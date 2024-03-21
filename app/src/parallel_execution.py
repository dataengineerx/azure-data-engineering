import concurrent.futures 
import datetime as dt
import random 

def sum(a,b):
    print(f"sum of {a} and {b} is equals to {a+b} at {dt.datetime.now()}")
    return a+b

total_threads = 5 

def main():
    with concurrent.futures.ThreadPoolExecutor(max_workers=total_threads) as executor:
        futures = [executor.submit(sum, random.randint(1,100), random.randint(100,200)) for i in range(total_threads)] 
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
        return results

import pendulum
# pendulum.tz.timezone("UTC")
pendulum.timezone()

if __name__ == "__amain__":
    main()

