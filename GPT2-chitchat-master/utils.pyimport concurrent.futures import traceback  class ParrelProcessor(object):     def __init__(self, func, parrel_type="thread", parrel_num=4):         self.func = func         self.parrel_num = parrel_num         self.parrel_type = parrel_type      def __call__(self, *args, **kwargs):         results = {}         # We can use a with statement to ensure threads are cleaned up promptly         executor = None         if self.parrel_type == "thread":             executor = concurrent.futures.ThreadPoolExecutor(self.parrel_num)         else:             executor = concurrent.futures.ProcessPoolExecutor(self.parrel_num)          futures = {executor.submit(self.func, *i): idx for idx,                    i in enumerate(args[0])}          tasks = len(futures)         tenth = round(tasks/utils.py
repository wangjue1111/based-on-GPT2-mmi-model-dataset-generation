import concurrent.futures
import traceback

class ParrelProcessor(object):
    def __init__(self, func, parrel_type="thread", parrel_num=4):
        self.func = func
        self.parrel_num = parrel_num
        self.parrel_type = parrel_type

    def __call__(self, *args, **kwargs):
        results = {}
        # We can use a with statement to ensure threads are cleaned up promptly
        executor = None
        if self.parrel_type == "thread":
            executor = concurrent.futures.ThreadPoolExecutor(self.parrel_num)
        else:
            executor = concurrent.futures.ProcessPoolExecutor(self.parrel_num)

        futures = {executor.submit(self.func, *i): idx for idx,
                   i in enumerate(args[0])}

        tasks = len(futures)
        tenth = round(tasks / 10)
        print('Formed pool of {} tasks'.format(tasks))

        for idx, future in enumerate(concurrent.futures.as_completed(futures)):
            i = futures[future]
            try:
                # store result
                data = future.result()
                # check to see if in array form

                results[i] = data
            except Exception as exc:
                print('{} generated an exception: {}'.format(
                    args[0][i], traceback.format_exc()))

            if tenth != 0 and idx != 0 and idx % tenth == 0:
                print('{}% Done'.format(round(idx / tasks, 2) * 100))

        executor.shutdown(wait=True)
        # sort and put in array
        final = []
        for k, v in sorted(results.items()):
            final.append(v)

        return final

def get_weights(datas):
    new_datas=[]
    weights=[]
    for data in datas.splitlines():
        item=data.split('$')
        new_datas.append(item[0])
        weights.append(int(item[1]))
    return new_datas,weights



def merge_data(files, name):
    with open(name, 'w') as w:
        for file in files:
            w.write(open(file).read())
