import string
import decimal
import operator

def determine_color(avg_prevalence):
  if(avg_prevalence <= 25):
    return 'prev1'
  elif(avg_prevalence <= 50):
    return 'prev2'
  elif(avg_prevalence <= 75):
    return 'prev3'
  else:
    return 'prev4'

li = []
i = 0
for l in f.readlines():
  li.append(l.strip())

dir_list = []
for l in li:
  line = l.split('::')
  line[1] = decimal.Decimal(line[1])
  line[2] = decimal.Decimal(line[2])
  line[3] = decimal.Decimal(line[3])
  dir_list.append(line)

#dir_list = sorted(dir_list, key=lambda x: (x[3], -x[2])) #dirs with low average prevalence
#dir_list = sorted(dir_list, key=lambda x: (-x[3], -x[2])) #dirs with high average prevalence
dir_list = sorted(dir_list, key=lambda x: (-x[2])) #dirs with the highest file count

x = 1
for l in dir_list:
  #if l[3] < 25 or l[3] > 75:
  #  continue;
  line = "chr - dir" + str(x) + " " + l[0].replace(' ', '_') + " 0 " + str(l[2]) + " " + determine_color(l[3]) + "\n"
  of.write(line)
  x = x + 1
  if x == 300:
   break

f.close()
of.close()
