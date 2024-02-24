"""explodes the dbc files from databricks into more useful python/sql/markdown files."""
from __future__ import print_function
import json
import sys
import os
import zipfile

def getLangPrefix(cmdstr):
  prefix = cmdstr.splitlines()[0] if len(cmdstr) > 0 else ''

  if len(prefix) > 0 and prefix[0] == '%':
    prefix = prefix[1:]
  else:
   prefix = ''

  return prefix
    
def getExtension(notebook):
  extMap = {
    'python': 'py',
    'md': 'md',
    'sql': 'sql',
    'scala': 'scala',
  }
  return extMap.get(notebook['language'])
      
def outdir(inputFile):
  outdir = inputFile + '-exploded'
  if not (os.path.exists(outdir) and os.path.isdir(outdir)):
    os.mkdir(outdir)
  return outdir

def processjsonfile(filepath):
  with open(filepath) as f:
    try:
      notebook = json.loads(f.read())
    except ValueError as e:
      notebook = None
      pass

  # ensure it is a notebook
  if notebook == None or (not notebook['version'] == 'NotebookV1'):
    print('SKIPPING file, ', filepath, '. Not a notebook.')
    return
  
  notebookName = notebook['name']
  commands = notebook['commands']

  # prepare output dir:
  dir = os.path.dirname(filepath)
  ext = getExtension(notebook)
  outfilepath = os.path.join(dir, notebookName + '.' + ext)

  print(os.path.basename(filepath), '->', os.path.basename(outfilepath))

  with open(outfilepath, 'wb') as f:
    f.write(('# Databricks notebook source\n').encode('utf-8'))
    first = True
    for command in commands:
      commandNo = command['position']
      # Skip if not a whole number
      if (not commandNo.is_integer()):
        continue

      if (first):
        first = False
      else:
        f.write(('\n# COMMAND ----------\n\n').encode('utf-8'))


      cmdstr = command['command']
      if len(cmdstr) > 0:      
        cmdlines = cmdstr.splitlines()
        for cmdline in cmdlines:
          f.write(('# MAGIC ').encode('utf-8') + cmdline.encode('utf-8') + ('\n').encode('utf-8'))

  os.remove(filepath)



def iszipfile(filepath):
  return zipfile.is_zipfile(filepath);

def processdir(filepath, deleteFileAfter=False):
  for dir, dirs, files in os.walk(filepath):
      for filepath in files:
        fullpath=os.path.join(dir, filepath)
        processjsonfile(fullpath)
        if deleteFileAfter: os.remove(fullpath)

def processzipfile(filepath):
  import tempfile
  from tempfile import mkdtemp
  from zipfile import ZipFile
  destDir = tempfile.mkdtemp()
  with ZipFile(filepath, 'r') as dbc:
    dbc.extractall(destDir)
  processdir(destDir, deleteFileAfter=False)
  from shutil import move
  move(destDir, filepath + '-exploded')
  
def main():
  if len(sys.argv) != 2:
    print('sys.argv', sys.argv)
    print("""
    Usage: dbc-explode <dbc_file>

    Run with example jar:
    dbc-explode /path/file.dbc
    """, file=sys.stderr)
    exit(-1)
  
  #load file:
  filepath = os.path.abspath(sys.argv[1])
  if os.path.isfile(filepath):
    if iszipfile(filepath):
      processzipfile(filepath)
    else:
      processjsonfile(filepath)
  elif os.path.isdir(filepath):
    processdir(filepath)


if __name__ == "__main__":
  main()
