import './App.css'
import fileMap from './fileMapProd.json'

import { ChonkyIconFA } from 'chonky-icon-fontawesome'

import {
  ChonkyActions,
  FileHelper,
  FullFileBrowser,
  setChonkyDefaults
} from 'chonky'
import React, { useMemo, useState } from 'react'

setChonkyDefaults({
  disableDragAndDrop: true,
  iconComponent: ChonkyIconFA
})

export const useFiles = (
  fileMap,
  currentFolderId
) => {
  return useMemo(() => {
    const currentFolder = fileMap[currentFolderId]
    const childrenIds = currentFolder.childrenIds
    const files = childrenIds.map((fileId) => fileMap[fileId])
    return files
  }, [currentFolderId, fileMap])
}

export const useFolderChain = (
  fileMap,
  currentFolderId
) => {
  return useMemo(() => {
    const currentFolder = fileMap[currentFolderId]

    const folderChain = [currentFolder]

    let parentId = currentFolder.parentId
    while (parentId) {
      const parentFile = fileMap[parentId]
      if (parentFile) {
        folderChain.unshift(parentFile)
        parentId = parentFile.parentId
      } else {
        break
      }
    }

    return folderChain
  }, [currentFolderId, fileMap])
}

function App () {
  const [presentInChunks, setPresentInChunks] = useState(false)
  const [nSelectedFiles, setNSelectedFiles] = useState(0)
  const [currentFolderId, setCurrentFolderId] = useState('0')
  const files = useFiles(fileMap.fileMap, currentFolderId)
  const folderChain = useFolderChain(fileMap.fileMap, currentFolderId)

  const handleFileAction = (data) => {
    if (data.id === 'change_selection') {
      const selectedFiles = data.state.selectedFiles
      // Iterate over selected files and build a set of chunks from the property ['presentInChunks']
      const chunks = new Set()
      selectedFiles.forEach((file) => {
        const presentInChunks = file.presentInChunks
        presentInChunks.forEach((chunk) => {
          chunks.add(chunk)
        })
      })
      setPresentInChunks(Array.from(chunks))
      setNSelectedFiles(selectedFiles.length)
    }

    if (data.id === ChonkyActions.OpenFiles.id) {
      const { targetFile, files } = data.payload
      const fileToOpen = targetFile ?? files[0]

      if (fileToOpen && FileHelper.isDirectory(fileToOpen)) {
        setCurrentFolderId(fileToOpen.id)
      }
    }
  }

  let selectedFilesStr = ''

  let filesSuffix = ''
  let chunksSuffix = ''
  if (nSelectedFiles > 1) {
    filesSuffix = 's'
  }

  if (presentInChunks) {
    if (presentInChunks.length > 1) {
      chunksSuffix = 's'
    }
    selectedFilesStr = `Selected file${filesSuffix} present in chunk${chunksSuffix}: ${presentInChunks.join(', ')}`
  }

  return (
    <div>
      <div style={{ padding: 20 }}>
        {selectedFilesStr}
      </div>
      <div style={{ height: 1200 }}>
        <FullFileBrowser
          files={files}
          folderChain={folderChain}
          onFileAction={handleFileAction}
        />
      </div>
    </div>
  )
}

export default App
