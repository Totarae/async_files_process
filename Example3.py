import random
import os
import zipfile

from lxml import etree
import asyncio
import aiofiles
import string

FILES_IN_ARCHIVE = 2
FILES_TOTAL = 10
TOTAL_ZIPS = 5


async def make_file(file_path):
    # start XML
    root = etree.Element('root')

    root.append(etree.Element('var', name='id', value=str(''.join(random.choices(string.ascii_letters, k=7)))))
    # another child with text
    root.append(etree.Element('var', name='level', value=str(random.randint(0, 100))))

    parent = etree.Element('objects')
    for i in range(0, random.randint(0, 9)):
        etree.SubElement(parent, 'object', name=str(''.join(random.choices(string.ascii_letters, k=7))))
    root.append(parent)

    tree = etree.ElementTree(root)
    await asyncio.sleep(random.randint(0, 5))

    absolute_path = os.path.abspath(file_path)
    try:
        os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
        async with aiofiles.open(absolute_path, "wb") as file:
            content = etree.tostring(tree, encoding="utf-8", xml_declaration=True)
            await file.write(content)

        print(f"File created: {absolute_path}")
        return {
            'state': True,
            'file_path': absolute_path
        }
    except IOError:
        print(f"Error creating file: {absolute_path}")
        return {
            'state': False,
            'file_path': absolute_path
        }


async def consume(queue, index):
    while True:
        # wait for an item from the producer
        files_to_zip = []
        for i in range(FILES_IN_ARCHIVE):
            item = await queue.get()

            # process the item
            if item['state']:
                print(f'Consumer {index} processing file: {item["file_path"]}, queue_size={queue.qsize()} ')
                files_to_zip.append(item["file_path"])
            queue.task_done()
            # simulate i/o operation using sleep
            # await asyncio.sleep(random.random())

        os.makedirs('zipped', exist_ok=True)
        zip_file = zipfile.ZipFile(f'zipped/archive_{index}.zip', 'w', zipfile.ZIP_DEFLATED)

        for file in files_to_zip:
            file_path = os.path.join("output", file)  # Join the file name with the path
            print(f'To be zipped: {file_path} for archive: {zip_file.filename}')
            arcname = os.path.basename(file)  # Get only the filename without the directory structure
            zip_file.write(file_path, arcname=arcname)

            # Delete the file from the "output" directory
            #os.remove(file_path)

        zip_file.close()
        # Notify the queue that the item has been processed
        print(f'task: {index} Done! , queue_size={queue.qsize()}')
        #if index == TOTAL_ZIPS-1 and queue.empty():
            #pending = asyncio.all_tasks()
            #for task in pending:
                #print(task)
            #break


async def produce(queue, n):
    for x in range(1, n + 1):
        filename = f'file_{x}.xml'
        file_path = os.path.join("output", filename)
        # produce an item
        #print(f'producing {x}/{n}')
        item = await make_file(file_path)
        # put the item in the queue
        await queue.put(item)


async def main():
    queue = asyncio.Queue(maxsize=50)

    consumers = []
    for i in range(TOTAL_ZIPS):
        consumer = asyncio.create_task(consume(queue, i))
        print(f'Adding {i} consumer')
        consumers.append(consumer)

    # run the producer and wait for completion
    await produce(queue, FILES_TOTAL)
    # wait until the consumer has processed all items
    await queue.join()

    # the consumers are still awaiting for an item, cancel them
    for consumer in consumers:
        consumer.cancel()

    # wait until all worker tasks are cancelled
    await asyncio.gather(*consumers, return_exceptions=True)


asyncio.run(main())
