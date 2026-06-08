import asyncio

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
    "The ER is divided into sections based on its locations. The ER by the cell membrane is the peripheral ER. This connects to the tubular ER, which acts as pipes to transport materials across the cell. [KEEP PANNING] the tubular ER connects to the cisternal ER, which is closest to the nucleus and thus takes materials from it.",
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




texts = [
"These are cancerous epithelial cells. Right away we see that the cellular membrane has a sharper and more needle-like shape compared to the normal cells.",
"The organelles in the cancerous cells are also more clumped together and expanded.",
"Significant changes are also seen in the nucleus of the epithelial cells. The nuclear envelope and nucleolus of the cancerous epithelial cells are both enlarged compared to normal epithelial cells.",
"Being able to tell the changes of various cells allows medical practitioners to tell when your body has diseases and allows them to find effective treatments."
]



texts = [
"These are cancerous epithelial cells. Right away we see that the cellular membrane has a sharper and more needle-like shape compared to normal epithelial cells.",
"The organelles in the cancerous cells are also more clumped together and expanded.",
"Significant changes are also seen in the nucleus of the epithelial cells. The nuclear envelope and nucleolus of the cancerous epithelial cells are both enlarged compared to normal epithelial cells.",
"Being able to tell the changes of various cells allows medical practitioners to tell when your body has diseases and allows them to find effective treatments."
]




texts = [
    "The red stained glass represents organelles inside of neutrophils that have a high refractive index. ",
    "In real cell scans, it’s not always possible to determine exactly what each organelle is. So instead, we mark different locations of the cells based on how fast light  travels through them..",
    "Structures with high refractive indices in the neutrophils are composed mostly of the nucleus, granules, and mitochondria, which are the most abundant structures in the neutrophils.",
    "The blue stained glass represents structures that have a low refractive index, including the membrane and cytosol of the neutrophils. ",
    "Neutrophils are one of the most common types of white blood cells, and they are responsible for defending the body against infections. Neutrophils use a process called phagocytosis, or “cell eating” to wrap around invasive species and destroy them.",
    "You will notice many destroyed neutrophils among the cells (shown by the floating structures of a broken cell) Neutrophils have short lives and are destroyed in multiple ways, such as netosis, where they break down without exposing toxic contents to other cells.",
    "Neutrophils are an important part of our immune system, and they play a crucial role in protecting our bodies from harmful pathogens."
]

texts = [
"Knowing the shape and function of the different organelles can help medical workers identify abnormalities in cells. These cells are non-cancerous epithelial breast cells.",
"The light blue glass showing the outer layer of the cell represents the cell membrane of the cell.",
"These lime structures are various organelles of the epithelial cells, each performing different functions for the cell.",
"This is a nucleus of the epithelial cell. The pink stained glass represents the nuclear envelope while the purple concrete represents the nucleolus, which produces ribosomes for the cell."
]

texts = [
    "This representation of the minimal cell was reconstructed from a computer simulation. Each block is 0.3 nanometers.",
    "Simulating the minimal cell provides insight into how it works, allowing us to track individual molecules and observe their interactions.",
    "Lime green glass represents the cell membrane of the minimal cell. The membrane allows small molecules like water and sugar to flow in and out of the cell. Larger materials need to be allowed in by membrane proteins.",
    "The blue concrete in this cell represents membrane proteins. Membrane proteins are proteins in the membrane which control how the cell interacts with its environment. These membrane proteins are transport proteins which allow certain molecules to enter and exit the cell as needed.",
    "Molecules that the cells need will be let in through the membrane proteins, just like how we are passing through to enter the cell.",
    "The inside of the minimal cell contains cytosolic proteins and chromosomes.",
    "In this case, the chromosomes are represented by glowstone. Chromosomes are structures of organized DNA. If DNA is like a page of instructions teaching the cell how to build a protein, then chromosomes are like books that organize the pages and hold them together.",
    "The red concrete represents cytosolic proteins. Cytosolic proteins are proteins inside of the cell that perform functions that the cells need to survive.",
    "Each of these components work together to allow the minimal cell to sustain life."
]

texts = [
    "This is an updated representation of the minimal cell using the Martini 3 simulation. Each block is 1 nanometer.",
    "Lime stained glass represents the nuclear membrane in this cell. The membrane allows small particles such as water and carbon dioxide) to flow in and others to flow out. ",
    "Larger materials and charged particles may need to be allowed in by membrane proteins called transporters.",
    "Represented by blue concrete, membrane proteins are proteins embedded in the cell's membrane to control how the cell interacts with its environment.",
    "In the minimal cell, there are over 20 different types of membrane proteins to let specific materials in or out of the cell.",
    "The cell's chromosomes are represented by yellow concrete. Chromosomes are structures of organized DNA. If DNA is like a page of instructions teaching the cell how to build a protein, then chromosomes are like books that organize the pages and hold them together.",
    "Cyan concrete represents the cell's RNA, which stands for ribonucleic acid. Both RNA and DNA fall under the class of nucleic acids.",
    "One of the most important functions of RNA is to act as a bridge between DNA and proteins. Because RNA can store information like DNA does and perform tasks like proteins do, RNA is responsible for carrying instructions from DNA to ribosomes and helping them interpret these instructions to construct proteins.",
    "The red concrete represents cytosolic proteins. Cytosolic proteins are proteins inside of the cell that perform functions that the cells need to survive. This includes tasks from building and repairing structures to breaking down molecules and processing energy.",
    "One function of the cytosolic proteins is to process metabolites, which are represented with purple concrete. Metabolites are small molecules such as water, sugars, and amino acids that a cell uses to survive.",
    "These small molecules are used in chemical reactions. For example, sugars are broken down to produce energy, while amino acids are used to build proteins for the cell.",
    "Together, the chemical reactions in a cell are known as the cell's metabolism."
]

texts = [
    "This is an updated representation of the minimal cell using the Martini 3 simulation. Each block is half a nanometer.",
]

# OUTPUT_FILE = "voice_converter/test.mp3"

async def amain() -> None:
    """Main function"""
    # communicate = edge_tts.Communicate(TEXT, VOICE)
    # await communicate.save(OUTPUT_FILE)
    
    for i in range(len(texts)):
        text = texts[i]
        output_path = f"voice_converter/martini3/ryan{i}.mp3"
        communicate = edge_tts.Communicate(text, VOICE)
        await communicate.save(output_path)

if __name__ == "__main__":
    asyncio.run(amain())