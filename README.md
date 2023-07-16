Assignment below:
================
Write a program in Python that does the following:

1. Creates 50 zip archives, each containing 100 xml files with random data of the following structure:
```xml
<root>
	<var name='id'
	     value='<random unique string value>'/>
	<var name='level'
	     value='<random number from 1 to 100>'/>
	<objects>
		<object name='<random string value>'/>
		<object name='<random string value>'/>...
    </objects>
</root>
```

The objects tag has a random number (1 to 10) of nested object tags.

2. Processes the directory with the received zip archives, parses the attached xml files and generates 2 csv files:First: id, level - one line for each xml fileSecond: id, object_name - one line for each object tag (1 to 10 lines for each xml file).