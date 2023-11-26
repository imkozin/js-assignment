const fs = require('fs')
const readline = require('readline')

async function readAndSortBlocks(filePath) {
  const blockSize = 500 * 1024 * 1024 // 500MB
  const blocks = []

  const stream = fs.createReadStream(filePath, { encoding: 'utf-8' })
  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Infinity,
  })

  let currentBlock = ''
  for await (const line of rl) {
    currentBlock += line + '\n'

    if (Buffer.from(currentBlock).length > blockSize) {
      blocks.push(currentBlock)
      currentBlock = ''
    }
  }

  if (currentBlock.length > 0) {
    blocks.push(currentBlock)
  }

  return blocks
}

async function sortAndWriteBlocks(blocks) {
  const sortedBlocks = await Promise.all(blocks.map(sortBlock))
  const sortedFilePaths = []

  for (let i = 0; i < sortedBlocks.length; i++) {
    const filePath = `sorted_block_${i}.txt`
    sortedFilePaths.push(filePath)

    fs.writeFileSync(filePath, sortedBlocks[i])
  }

  return sortedFilePaths
}

async function sortBlock(block) {
  const lines = block.trim().split('\n')
  const sortedLines = lines.sort()

  return sortedLines.join('\n')
}

async function mergeSortedBlocks(sortedFilePaths, outputFilePath) {
  const streams = sortedFilePaths.map((filePath) =>
    fs.createReadStream(filePath, { encoding: 'utf-8' })
  )
  const writers = fs.createWriteStream(outputFilePath, { encoding: 'utf-8' })

  await Promise.all(
    streams.map((stream) => new Promise((resolve) => stream.on('end', resolve)))
  )

  await mergeStreams(streams, writers)

  // Удаляем временные файлы
  sortedFilePaths.forEach((filePath) => fs.unlinkSync(filePath))
}

async function mergeStreams(streams, writer) {
  const values = []
  const readers = streams.map((stream) => {
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity })
    const iterator = rl[Symbol.asyncIterator]()

    return { iterator, value: null, done: false }
  })

  // Заполняем начальные значения
  await Promise.all(
    readers.map(async (reader) => {
      reader.value = await reader.iterator.next()
    })
  )

  while (readers.some((reader) => !reader.done)) {
    const minValue = readers
      .filter((reader) => !reader.done)
      .reduce((min, reader) => {
        if (!min || reader.value.value < min.value.value) {
          return { reader, value: reader.value.value }
        }
        return min
      }, null)

    if (minValue) {
      const { reader, value } = minValue
      values.push(value)

      reader.value = await reader.iterator.next()
      if (reader.value.done) {
        reader.done = true
      }
    }

    if (values.length >= 10000) {
      writer.write(values.join('\n') + '\n')
      values.length = 0
    }
  }

  if (values.length > 0) {
    writer.write(values.join('\n') + '\n')
  }

  writer.end()
}

async function main() {
  const inputFile = '/Users/ivankozin/vscode/js-assignment/mqtt_log.csv'
  const outputFilePath = '/Users/ivankozin/vscode/js-assignment/log.csv'

  const blocks = await readAndSortBlocks(inputFile)
  const sortedFilePaths = await sortAndWriteBlocks(blocks)
  await mergeSortedBlocks(sortedFilePaths, outputFilePath)

  console.log('Файл успешно отсортирован:', outputFilePath)
}

main()
