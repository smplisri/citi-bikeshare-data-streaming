import sys

def exception_catcher(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            print(e)
            sys.exit(1)
    return wrapper
