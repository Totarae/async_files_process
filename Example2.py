import csv
import os
import zipfile
import aiofiles
import asyncio
from lxml import etree
import random
import string
from random import randint

new_data = None
event = asyncio.Event()


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


async def task_a():
    tasks = []
    for i in range(0, 10):
        filename = f'file_{i}.xml'
        file_path = os.path.join("output", filename)
        task = make_file(file_path)  # Create the task without awaiting
        tasks.append(task)
    await asyncio.gather(*tasks)  # Await all tasks together
    event.set()


async def task_b():
    await event.wait()
    for file in os.listdir("output"):
        if file.endswith(".xml"):
            print(os.path.join("output", file))
    event.clear()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.create_task(task_a())
        loop.create_task(task_b())
        loop.run_forever()
    except KeyboardInterrupt:
        # Stop the event loop gracefully on keyboard interrupt (Ctrl+C)
        for task in asyncio.all_tasks():
            task.cancel()
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks()))
    finally:
        # Close the event loop when finished
        loop.close()
