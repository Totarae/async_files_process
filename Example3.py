import csv
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
            # os.remove(file_path)

        zip_file.close()
        # Notify the queue that the item has been processed
        print(f'task: {index} Done! , queue_size={queue.qsize()}')
        # if index == TOTAL_ZIPS-1 and queue.empty():
        # pending = asyncio.all_tasks()
        # for task in pending:
        # print(task)
        # break


async def produce(queue, n):
    for x in range(1, n + 1):
        filename = f'file_{x}.xml'
        file_path = os.path.join("output", filename)
        # produce an item
        # print(f'producing {x}/{n}')
        item = await make_file(file_path)
        # put the item in the queue
        await queue.put(item)


async def gatherArchives(queue):
    for i in range(TOTAL_ZIPS):
        item = f'zipped/archive_{i}.zip'
        await queue.put(item)

async def parse_xml_file(zip_file, xml_file):
    try:
        with zipfile.ZipFile(zip_file, 'r') as archive:
            xml_content = archive.read(xml_file)
    except Exception as e:
        print(f"Error reading XML file {xml_file} from zip file {zip_file}: {e}")
        return None, None, []

    try:
        root = etree.fromstring(xml_content)
        id_value_element = root.find('.//var[@name="id"]')
        level_value_element = root.find('.//var[@name="level"]')

        id_value = id_value_element.get('value') if id_value_element is not None else None
        level_value = level_value_element.get('value') if level_value_element is not None else None

        object_names = [obj.get('name') for obj in root.findall('.//objects/object')]

        return id_value, level_value, object_names
    except Exception as e:
        print(f"Error parsing XML file {xml_file} from zip file {zip_file}: {e}")
        return None, None, []


async def resultFiles(queue):
    zip_files = [f'zipped/archive_{i}.zip' for i in range(TOTAL_ZIPS)]

    id_level_lines = []
    id_object_lines = []
    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, 'r') as archive:
                xml_files = [name for name in archive.namelist() if name.endswith('.xml')]
                for xml_file in xml_files:
                    id_value, level_value, object_names = await parse_xml_file(zip_file, xml_file)
                    if id_value and level_value:
                        id_level_lines.append([id_value, level_value])
                    for object_name in object_names:
                        if object_name:
                            id_object_lines.append([id_value, object_name])
        except Exception as e:
            print(f"Error parsing zip file {zip_file}: {e}")

    # Write id, level lines to the first CSV file
    with open('id_level.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(id_level_lines)

    # Write id, object_name lines to the second CSV file
    with open('id_object.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(id_object_lines)

    print("CSV files generated")


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

    while not queue.empty():
        queue.get_nowait()
    # new interchange
    await asyncio.gather(gatherArchives(queue), resultFiles(queue))


asyncio.run(main())
