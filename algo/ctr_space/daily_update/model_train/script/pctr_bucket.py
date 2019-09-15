import sys
import math

class pctr_bucket:
    def __init__(self, size=500):
        self.bias_click_threshold = 100
        self.bias_size_threshold = 200
        self.size = size
        self.click_data = [0] * self.size
        self.ed_data = [0] * self.size
        self.pctr_data = [0] * self.size

    def add_pctr(self, pctr, label):
        pos = int(math.floor(float(pctr) * self.size))
        if pos > self.size / 2:
            pos = self.size / 2
        self.click_data[pos] += int(label)
        self.ed_data[pos] += 1
        self.pctr_data[pos] += float(pctr)

    def show_bucket(self, prefix=''):
        str = ''#'bucket[%s] '% prefix
        for i in range(self.size/2):
            click = self.click_data[i]
            ed = self.ed_data[i]
            # ctr = click * 1.0 / ed if ed > 0 else 0
            sum_pctr = self.pctr_data[i]
            diff = 0 if click == 0 else (sum_pctr - click) * 1.0 / click
            str += '%i %i %i %.4f %.4f\n' % (i, click, ed, sum_pctr, diff)
        return str

    def get_bias(self):
        bias_list = []
        for i in range(self.bias_size_threshold):
            click = self.click_data[i]
            if click < self.bias_click_threshold:
                continue
            bias = abs(self.pctr_data[i] - click)/click
            bias_list.append(bias)
        if len(bias_list) > 0:
            return sum(bias_list)/len(bias_list)
        return 0

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: py pred.txt"
        exit()
    pb = pctr_bucket()
    for raw_line in open(sys.argv[1]):
        line = raw_line.strip("\n\r ").split()
        pb.add_pctr(line[0], line[1])
    print pb.show_bucket()
