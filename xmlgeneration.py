import os
import zipfile

import asyncio
from lxml import etree
import random
import string


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
        with open(absolute_path, "wb") as file:
            content = etree.tostring(tree, encoding="utf-8", xml_declaration=True)
            file.write(content)

        print(f"File created: {absolute_path}")
        return True
    except IOError:
        print(f"Error creating file: {absolute_path}")
        return False


def create_zip(files, zip_name):
    with zipfile.ZipFile(zip_name, "w") as zip_file:
        for file in files:
            zip_file.write(file, os.path.basename(file))
    print(f"Zip file created: {zip_name}")





async def main():
    # Define the callback function

    def callback(task):
        if task.exception() is None:
            result = task.result()
            print(f"Callback: Last task completed with result: {result}")
        else:
            exception = task.exception()
            print(f"Callback: Last task raised an exception: {exception}")

        # Check if the task completed successfully
        if task.exception() is None:
            # Get the file paths of completed tasks
            completed_files = [t.get_name() for t in tasks if t.done() and t.exception() is None]

            # Create zip file with completed file paths
            zip_name = "output.zip"
            create_zip(completed_files, zip_name)

    tasks = []
    for i in range(100):
        file_name = f"file_{i}.xml"
        file_path = os.path.join("output", file_name)
        task = asyncio.create_task(make_file(file_path))
        task.set_name(file_path)
        tasks.append(task)
    await asyncio.gather(*tasks)

    # Add callback to the last task
    last_task = tasks[-1]
    last_task.add_done_callback(callback)


asyncio.run(main())
