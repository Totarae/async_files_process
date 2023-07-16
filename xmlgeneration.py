import os
import asyncio
from lxml import etree
import random
import string


async def makeFile(file_path):
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
    await asyncio.sleep(random.randint(0,5))

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


async def main():
    tasks = []
    for i in range(100):
        file_name = f"file_{i}.xml"
        file_path = os.path.join("output", file_name)
        tasks.append(makeFile(file_path))
    await asyncio.gather(*tasks)

asyncio.run(main())
