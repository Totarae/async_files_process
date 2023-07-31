import csv
import os
import zipfile
import aiofiles
import asyncio
from lxml import etree
import random
import string
from random import randint

FILES_IN_ARCHIVE = 10
TOTAL_ZIPS = 5
CLEAN_UP = False

new_data = None
events = [asyncio.Event() for _ in range(TOTAL_ZIPS)]  # Create pool of events


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
        return True
    except IOError:
        print(f"Error creating file: {absolute_path}")
        return False


async def task_a(event_index):
    tasks = []
    for i in range(0, FILES_IN_ARCHIVE):
        filename = f'file_{event_index}_{i}.xml'
        file_path = os.path.join("output", filename)
        task = make_file(file_path)  # Create the task without awaiting
        tasks.append(task)
    await asyncio.gather(*tasks)  # Await all tasks together
    events[event_index].set()


async def task_b(event_index):
    await events[event_index].wait()
    os.makedirs('zipped', exist_ok=True)
    zip_file = zipfile.ZipFile(f'zipped/archive_{event_index}.zip', 'w', zipfile.ZIP_DEFLATED)
    files_to_zip = os.listdir("output")[:FILES_IN_ARCHIVE]

    for file in files_to_zip:
        # if file.endswith(f"_{event_index}.xml"):
        # print('To be zipped:')
        file_path = os.path.join("output", file)  # Join the file name with the path
        print('To be zipped: ', file_path)
        zip_file.write(file_path, file)

        # Delete the file from the "output" directory
        os.remove(file_path)

    print('Zip archive created: ', zip_file.filename)
    zip_file.close()

    print(f'Completed {event_index} event')
    events[event_index].clear()


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


async def task_c():
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


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        # Create and await task_a tasks
        loop.run_until_complete(asyncio.gather(*(task_a(i) for i in range(TOTAL_ZIPS))))

        # Create and await task_b tasks
        tasks_b = [task_b(i) for i in range(TOTAL_ZIPS)]
        loop.run_until_complete(asyncio.gather(*tasks_b))

        final_task = loop.create_task(task_c())
        loop.run_until_complete(final_task)
    except KeyboardInterrupt:
        # Stop the event loop gracefully on keyboard interrupt (Ctrl+C)
        for task in asyncio.all_tasks():
            task.cancel()
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks()))
    finally:
        # Close the event loop when finished
        loop.close()
