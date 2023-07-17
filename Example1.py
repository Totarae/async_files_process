import os
import zipfile
import aiofiles
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
        async with aiofiles.open(absolute_path, "wb") as file:
            content = etree.tostring(tree, encoding="utf-8", xml_declaration=True)
            await file.write(content)

        print(f"File created: {absolute_path}")
        return True
    except IOError:
        print(f"Error creating file: {absolute_path}")
        return False


async def create_zip_archive(zip_filename, xml_files):
    zip_file = zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED)
    for xml_file in xml_files:
        zip_file.write(xml_file)
    zip_file.close()
    print(f"Zip archive created: {zip_filename}")


async def create_zip_archives():
    xml_files = [f'output/file_{i}.xml' for i in range(5000)]
    random.shuffle(xml_files)

    tasks = []
    for i in range(50):
        start_index = i * 100
        end_index = (i + 1) * 100
        zip_filename = f'zipped/archive_{i}.zip'
        task = asyncio.create_task(create_zip_archive(zip_filename, xml_files[start_index:end_index]))
        tasks.append(task)
    await asyncio.gather(*tasks)


async def create_files():
    tasks = []
    for i in range(5000):
        filename = f'file_{i}.xml'
        file_path = os.path.join("output", filename)
        task = asyncio.create_task(make_file(file_path))
        tasks.append(task)
    await asyncio.gather(*tasks)


async def main():
    await create_files()
    await create_zip_archives()


asyncio.run(main())
