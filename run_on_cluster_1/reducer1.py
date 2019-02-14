import sys

current_pair = None
current_count = 0
pair = None

for line in sys.stdin:
    
    line = line.strip()
    pair, count = line.split('\t', 1)
 
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
 
    if current_pair == pair:
        current_count += count
    else:
        if current_pair:
            # write result to STDOUT
            print ('%s\t%s' % (current_pair, current_count))
        current_count = count
        current_pair = pair

if current_pair == pair:
        print ('%s\t%s' % (current_pair, current_count))
    