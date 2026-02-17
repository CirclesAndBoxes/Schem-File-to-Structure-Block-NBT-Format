import asyncio
import random

import edge_tts
from edge_tts import VoicesManager


# async def amain() -> None:
#     """Main function"""
#     voices = await VoicesManager.create()
#     voice = voices.find(Gender="Male", Language="es")
#     # Also supports Locales
#     # voice = voices.find(Gender="Female", Locale="es-AR")

#     communicate = edge_tts.Communicate(TEXT, random.choice(voice)["Name"])
#     await communicate.save(OUTPUT_FILE)

# if __name__ == "__main__":
#     asyncio.run(amain())

TEXT = """
The structures made with yellow wool represent the ribosomes. Ribosomes take instructions from the DNA to build proteins for the cell to function.

"""
VOICE = "en-GB-RyanNeural"
# Tried:
# en-GB-SoniaNeural
# en-US-AndrewNeural -- He is pretty good! Except he can't say 'lime' , 'wool', etc.
# en-US-RogerNeural -- He sounds like AI... Not great. 
# en-GB-RyanNeural -- Also sounds like AI but it sounds pretty good, like you aren't trying to hide it
# en-US-EmmaMultilingualNeural -- She sounds pretty good. A little robotic. Some sounds are a little off + speeds off.
# en-US-BrianMultilingualNeural -- A little too neutral. Sounds smooth though. Pretty good. 
# en-US-AndrewMultilingualNeural -- Better compared to first Andrew


# This is a representation of a minimal cell, where each block is 8 nanometers. 
# The lime green glass surrounding the minimal cell represents the cell membrane, which forms the border of the minimal cell. The cell membrane controls what can enter and leave, allowing nutrients and oxygen to move in while keeping dangerous toxins out.
# The red wool represents the minimal cell's DNA. The DNA in the minimal cell contains all the information the minimal cell needs to make proteins and replicate itself.



texts = [
    "In this yeast cell, each block is 28 nanometers. A yeast cell has many more different components, known as organelles, that serve different functions to allow the cell to survive.",
    "The glass surrounding the yeast cell represents the cell wall—a strong outer layer that surrounds and protects the yeast cell. The yeast cell wall gives the cell its shape, provides mechanical strength, and prevents it from bursting when water enters.",
    "Inside of the cell wall is the cell membrane, represented by lime stained glass. This is a soft, flexible layer that wraps around the cell. It controls which materials that enter and exit the cell.",
    "Continuing to work our way into the cell, we find the endoplasmic reticulum, or ER, represented by orange stained glass. The ER is responsible for transporting proteins and molecules across the cell and helping produce important molecules. ",
    "The ER is divided into sections based on its locations. The ER by the cell membrane is the peripheral ER. [PAN TO NEW LOCATION] This connects to the tubular ER, which acts as pipes to transport materials across the cell. [KEEP PANNING] the tubular ER connects to the cisternal ER, which is closest to the nucleus and thus takes materials from it.",
    "The nuclear envelope or nuclear membrane of the cell is shown by the red concrete. This protects the yeast cell's nucleus, or the “brain” of the cell. The nucleus contains all of the DNA for the cell, the instructions for creating various proteins and structures of the cell.",
    "The nuclear envelope has holes in it, known as nuclear pores, which control transport into and out of the nucleus.",
    "Blue concrete represents the chromosomes of the yeast cell. Yeast has 16 separate chromosomes, The chromosomes contain DNA, which stores the genetic information of the cell, along with proteins that allow the chromosome to hold its shape.",
    "Only eukaryotic cells, such as yeast, plant, and animal cells, have a nucleus. By storing all their genetic information in one space, eukaryotic cells can grow larger and carry out more complex tasks than bacteria cells.",
    "Other structures in the cell include the vacuole, a storage and recycling compartment inside the yeast cell. The vacuole stores nutrients and ions, helps break down and recycle cellular waste, and maintains the cell's internal balance (pH and ion levels).",
    "Unlike the large central vacuole in plant cells, yeast cells contain smaller vacuoles that play key roles in nutrient storage, detoxification, and stress response.",
    "The light blue concrete represents the mitochondria, known as the “powerhouse of the cell”. Mitochondria breaks down food molecules so its energy can be used by the cell.",
    "The yellow concrete floating around the yeast cell represents the ribosomes. These act as “protein factories” of the cell, floating around and assembling proteins based on instructions from the nucleus.",
    "In a real yeast cell, there are many more ribosomes than are shown in this model. We only show one percent of the ribosomes in a yeast cell to make the cell easier to maneuver and visualize."
]


OUTPUT_FILE = "voice_converter/test.mp3"

async def amain() -> None:
    """Main function"""
    # communicate = edge_tts.Communicate(TEXT, VOICE)
    # await communicate.save(OUTPUT_FILE)
    
    for i in range(len(texts)):
        text = texts[i]
        output_path = f"voice_converter/yeast/ryan{i}.mp3"
        communicate = edge_tts.Communicate(text, VOICE)
        await communicate.save(output_path)

if __name__ == "__main__":
    asyncio.run(amain())