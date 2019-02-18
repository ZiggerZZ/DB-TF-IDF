#!/usr/bin/python
import sys
# import subprocess
# subprocess.call('dir', shell=True)
current_file = None
current_term = None
term_list = []
current_term_count = 0
pair = None

for line in sys.stdin:
    line = line.strip()
    pair, count = line.split('\t', 1)
    file, term = pair.split('_', 1)

# convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    if current_file == file:
        if current_term == term:
            current_term_count += count
        else:
            term_list.append((current_term, current_term_count))
            current_term = term
            current_term_count = count

    else:
        term_list.append((current_term, current_term_count))
        if current_file:
            # write result to STDOUT
            for t, ct in term_list:
                print('%s\t%s' % (t+'_'+current_file,
                                  str(len(term_list))+'_'+(str(ct))))

        current_term_count = count
        current_file = file
        current_term = term
        term_list = []

if term_list:
    for t, ct in term_list:
        print('%s\t%s' % (t+'_'+current_file,
                          str(len(term_list)+1)+'_'+(str(ct))))

print('%s\t%s' % (current_term+'_'+current_file,
                  str(len(term_list)+1)+'_'+str(current_term_count)))
