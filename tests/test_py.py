import os

# Determine the log directory
current_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.normpath(os.path.join(current_dir, '../logs'))

print("Current dir :", current_dir)
print("logs dir : ", log_dir)
